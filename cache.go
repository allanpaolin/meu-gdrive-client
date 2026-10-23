package main

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

const (
	DefaultCacheLimit int64 = 15 * 1024 * 1024 * 1024

	// Geometria dos blocos: o bloco 0 é pequeno (barato de aquecer em massa e
	// suficiente para sniffing de tipo MIME / cabeçalhos); os demais têm 1 MiB.
	headSize  = int64(128 * 1024)
	blockSize = int64(1024 * 1024)

	demandSlots      = 8 // downloads disparados por Read (caminho crítico)
	prefetchSlots    = 8 // downloads de read-ahead/aquecimento (melhor esforço)
	fetchTimeout     = 45 * time.Second
	maxFetchAttempts = 4
)

// FileRef identifica uma versão específica de um arquivo. Size e MTime entram na
// chave do bloco: se o arquivo mudar no Drive, os blocos antigos deixam de valer.
type FileRef struct {
	ID    string
	Size  int64
	MTime int64
}

func (r FileRef) blockKey(idx int64) string {
	return fmt.Sprintf("%s.%d.%d.%d", r.ID, r.Size, r.MTime, idx)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// blockBounds devolve [start, end) do bloco idx.
func blockBounds(idx, total int64) (start, end int64) {
	if idx == 0 {
		return 0, min64(headSize, total)
	}
	start = headSize + (idx-1)*blockSize
	return start, min64(start+blockSize, total)
}

func blockIndexFor(off int64) int64 {
	if off < headSize {
		return 0
	}
	return 1 + (off-headSize)/blockSize
}

type blockEntry struct {
	key  string
	size int64
}

type FileCache struct {
	dir      string
	maxBytes int64
	service  *drive.Service

	// Índice LRU em memória: nenhuma operação de cache varre o diretório.
	mu    sync.Mutex
	index map[string]*list.Element
	lru   *list.List // frente = mais recente
	total int64

	sf          singleflight.Group
	demandSem   chan struct{}
	prefetchSem chan struct{}

	warmMu     sync.Mutex
	warmCancel context.CancelFunc
}

// NewFileCache cria o cache de blocos em dir (normalmente ~/.cache/meu-gdrive-client/blocks).
// maxBytes é o limite do LRU; <= 0 usa DefaultCacheLimit.
func NewFileCache(service *drive.Service, dir string, maxBytes int64) (*FileCache, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultCacheLimit
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("criar diretório do cache %q: %w", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("ajustar permissões do cache %q: %w", dir, err)
	}
	c := &FileCache{
		dir:         dir,
		maxBytes:    maxBytes,
		service:     service,
		index:       make(map[string]*list.Element),
		lru:         list.New(),
		demandSem:   make(chan struct{}, demandSlots),
		prefetchSem: make(chan struct{}, prefetchSlots),
	}
	if err := c.loadIndex(); err != nil {
		return nil, err
	}
	return c, nil
}

// loadIndex varre o diretório UMA vez, na inicialização.
func (c *FileCache) loadIndex() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return fmt.Errorf("listar blocos do cache: %w", err)
	}
	type item struct {
		key  string
		size int64
		mod  time.Time
	}
	var items []item
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Temporários órfãos e blocos do formato antigo ("id_N", sem pontos; a
		// geometria dos blocos mudou) são descartados.
		if strings.HasPrefix(name, ".") || !strings.Contains(name, ".") {
			_ = os.Remove(filepath.Join(c.dir, name))
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		items = append(items, item{name, info.Size(), info.ModTime()})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].mod.Before(items[j].mod) })
	for _, it := range items { // do mais antigo ao mais novo: o mais novo termina na frente
		c.index[it.key] = c.lru.PushFront(&blockEntry{key: it.key, size: it.size})
		c.total += it.size
	}
	return nil
}

func (c *FileCache) blockPath(key string) (string, error) {
	if key == "" || filepath.Base(key) != key {
		return "", fmt.Errorf("chave de bloco inválida para o cache")
	}
	return filepath.Join(c.dir, key), nil
}

func (c *FileCache) has(key string) bool {
	c.mu.Lock()
	_, ok := c.index[key]
	c.mu.Unlock()
	return ok
}

func (c *FileCache) forget(key string) {
	c.mu.Lock()
	if el, ok := c.index[key]; ok {
		c.total -= el.Value.(*blockEntry).size
		c.lru.Remove(el)
		delete(c.index, key)
	}
	c.mu.Unlock()
}

// register insere o bloco no índice e despeja os mais antigos se passar do limite.
// O custo é O(1) por bloco removido (antes era uma varredura + stat de todos os blocos).
func (c *FileCache) register(key string, size int64) {
	var victims []string
	c.mu.Lock()
	if el, ok := c.index[key]; ok {
		c.total -= el.Value.(*blockEntry).size
		c.lru.Remove(el)
	}
	c.index[key] = c.lru.PushFront(&blockEntry{key: key, size: size})
	c.total += size
	for c.total > c.maxBytes {
		back := c.lru.Back()
		if back == nil || back == c.lru.Front() {
			break
		}
		e := back.Value.(*blockEntry)
		c.lru.Remove(back)
		delete(c.index, e.key)
		c.total -= e.size
		victims = append(victims, e.key)
	}
	c.mu.Unlock()
	for _, k := range victims { // fora do lock; em Linux, remover arquivo aberto é seguro
		_ = os.Remove(filepath.Join(c.dir, k))
	}
}

// readCached lê apenas [lo,hi) do bloco em disco (e não o bloco inteiro).
func (c *FileCache) readCached(key string, lo, hi int64) ([]byte, bool) {
	c.mu.Lock()
	el, ok := c.index[key]
	if ok {
		c.lru.MoveToFront(el)
	}
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	path, err := c.blockPath(key)
	if err != nil {
		return nil, false
	}
	f, err := os.Open(path)
	if err != nil {
		c.forget(key)
		return nil, false
	}
	defer f.Close()
	buf := make([]byte, hi-lo)
	n, _ := f.ReadAt(buf, lo)
	if int64(n) != hi-lo { // bloco truncado/corrompido
		c.forget(key)
		_ = os.Remove(path)
		return nil, false
	}
	return buf, true
}

// ReadRange devolve [offset, offset+size) do arquivo, juntando blocos do cache/rede.
func (c *FileCache) ReadRange(ctx context.Context, ref FileRef, offset int64, size int) ([]byte, error) {
	if offset < 0 || size < 0 || ref.Size < 0 {
		return nil, fmt.Errorf("intervalo de leitura inválido")
	}
	if size == 0 || offset >= ref.Size {
		return []byte{}, nil
	}
	end := offset + int64(size)
	if end < offset || end > ref.Size {
		end = ref.Size
	}
	out := make([]byte, 0, end-offset)
	for pos := offset; pos < end; {
		idx := blockIndexFor(pos)
		bStart, bEnd := blockBounds(idx, ref.Size)
		segEnd := min64(end, bEnd)
		part, err := c.readFromBlock(ctx, ref, idx, pos-bStart, segEnd-bStart)
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
		pos = segEnd
	}
	return out, nil
}

func (c *FileCache) readFromBlock(ctx context.Context, ref FileRef, idx, lo, hi int64) ([]byte, error) {
	key := ref.blockKey(idx)
	if buf, ok := c.readCached(key, lo, hi); ok {
		return buf, nil
	}

	// O download roda desacoplado do ctx do Read: se o kernel interromper esta
	// leitura (FUSE_INTERRUPT), os outros waiters do singleflight não falham junto.
	ch := c.sf.DoChan(key, func() (interface{}, error) {
		c.demandSem <- struct{}{}
		defer func() { <-c.demandSem }()
		return c.downloadBlock(ref, idx)
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		data, ok := r.Val.([]byte)
		if !ok || hi > int64(len(data)) {
			return nil, fmt.Errorf("bloco %q menor que o esperado", key)
		}
		return data[lo:hi], nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *FileCache) downloadBlock(ref FileRef, idx int64) ([]byte, error) {
	key := ref.blockKey(idx)
	start, end := blockBounds(idx, ref.Size)
	if start >= end {
		return nil, fmt.Errorf("intervalo inválido para o bloco %q", key)
	}
	want := end - start

	var lastErr error
	for attempt := 0; attempt < maxFetchAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff(attempt))
		}
		data, err := c.fetchRange(ref.ID, start, end-1, want)
		if err == nil {
			if err := c.store(key, data); err != nil {
				log.Printf("Aviso: não foi possível gravar o bloco %q no cache: %v", key, err)
			}
			return data, nil
		}
		lastErr = err
		if !shouldRetryFetch(err) {
			break
		}
	}
	return nil, fmt.Errorf("baixar bloco %q: %w", key, lastErr)
}

func (c *FileCache) fetchRange(id string, start, last, want int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()

	call := c.service.Files.Get(id).Context(ctx)
	call.Header().Set("Range", fmt.Sprintf("bytes=%d-%d", start, last)) // Range é inclusivo
	resp, err := call.Download()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		// Servidor ignorou o Range: só serve se o corpo for exatamente o que queremos.
		if start != 0 || resp.ContentLength != want {
			return nil, fmt.Errorf("servidor ignorou o cabeçalho Range (HTTP 200)")
		}
	default:
		return nil, fmt.Errorf("resposta HTTP inesperada: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, want+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != want { // nunca cachear bloco truncado
		return nil, fmt.Errorf("bloco truncado: %d de %d bytes: %w", len(data), want, io.ErrUnexpectedEOF)
	}
	return data, nil
}

func (c *FileCache) store(key string, data []byte) error {
	path, err := c.blockPath(key)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.dir, ".block-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	c.register(key, int64(len(data)))
	return nil
}

// Prefetch aquece um bloco em segundo plano. Melhor esforço: se não houver vaga no
// pool de prefetch, descarta. Nunca ocupa vagas do pool de demanda.
func (c *FileCache) Prefetch(ref FileRef, idx int64) {
	start, end := blockBounds(idx, ref.Size)
	if idx < 0 || start >= end {
		return
	}
	key := ref.blockKey(idx)
	if c.has(key) {
		return
	}
	select {
	case c.prefetchSem <- struct{}{}:
	default:
		return
	}
	go func() {
		defer func() { <-c.prefetchSem }()
		c.sf.Do(key, func() (interface{}, error) { return c.downloadBlock(ref, idx) })
	}()
}

// WarmHeads baixa o bloco 0 (128 KiB) dos arquivos de uma pasta, na ordem da listagem,
// com paralelismo limitado. Quando o Nemo/GIO percorre a pasta de forma serial (stat +
// leitura inicial para descobrir o tipo / gerar miniatura), os blocos já estão em disco.
// Uma nova chamada cancela o aquecimento anterior (o usuário mudou de pasta).
func (c *FileCache) WarmHeads(refs []FileRef) {
	if len(refs) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.warmMu.Lock()
	if c.warmCancel != nil {
		c.warmCancel()
	}
	c.warmCancel = cancel
	c.warmMu.Unlock()

	go func() {
		for _, ref := range refs {
			key := ref.blockKey(0)
			if c.has(key) {
				continue
			}
			select {
			case c.prefetchSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func(ref FileRef, key string) {
				defer func() { <-c.prefetchSem }()
				if ctx.Err() != nil {
					return
				}
				c.sf.Do(key, func() (interface{}, error) { return c.downloadBlock(ref, 0) })
			}(ref, key)
		}
	}()
}

func shouldRetryFetch(err error) bool {
	if isRetryableDriveError(err) { // definido em db.go
		return true
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return false
	}
	return !errors.Is(err, context.Canceled)
}

func backoff(attempt int) time.Duration {
	d := 300 * time.Millisecond << (attempt - 1)
	return d + time.Duration(rand.Int63n(int64(250*time.Millisecond)))
}
