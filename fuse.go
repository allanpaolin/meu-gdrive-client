package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/sync/singleflight"
	"google.golang.org/api/drive/v3"
)

const driveFolderMIMEType = "application/vnd.google-apps.folder"

const (
	rootDirectoryID = "root"

	dirSnapshotTTL  = 15 * time.Second // quanto tempo uma listagem em memória vale
	warmMaxFiles    = 300              // máx. de arquivos aquecidos por pasta
	readAheadBlocks = 6
	slowOpThreshold = 100 * time.Millisecond

	apiOpTimeout        = 30 * time.Second // Mkdir / Remove / Rename no Drive
	uploadRetryInterval = 2 * time.Minute  // reenvio de uploads que falharam
	maxParallelUploads  = 3
)

// traceSlow loga operações FUSE lentas.
func traceSlow(op, detail string, start time.Time) {
	if d := time.Since(start); d >= slowOpThreshold {
		log.Printf("[lento] %s %s: %v", op, detail, d.Round(time.Millisecond))
	}
}

// safeName torna um nome do Drive válido como entrada de diretório POSIX.
func safeName(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	s = strings.ReplaceAll(s, "/", "∕") // U+2215
	if s == "." || s == ".." {
		s = "_" + s
	}
	return s
}

func uniqueName(taken map[string]fs.Node, name, id string) string {
	if _, dup := taken[name]; !dup {
		return name
	}
	short := id
	if len(short) > 6 {
		short = short[:6]
	}
	ext := filepath.Ext(name)
	return strings.TrimSuffix(name, ext) + " (" + short + ")" + ext
}

func isGoogleApp(mime string) bool {
	return strings.HasPrefix(mime, "application/vnd.google-apps.") && mime != driveFolderMIMEType
}

type driveFS struct {
	db         *sql.DB
	cache      *FileCache
	srv        *drive.Service
	stagingDir string

	mu   sync.Mutex
	dirs map[string]*dirSnapshot
	sf   singleflight.Group

	// Uploads e operações de escrita no Drive em andamento. O onExit espera por isto.
	uploadWG  sync.WaitGroup
	uploadSem chan struct{}
	upMu      sync.Mutex
	inflight  map[string]struct{} // staging_path -> em envio
}

func newDriveFS(db *sql.DB, cache *FileCache, srv *drive.Service, stagingDir string) *driveFS {
	return &driveFS{
		db:         db,
		cache:      cache,
		srv:        srv,
		stagingDir: stagingDir,
		dirs:       make(map[string]*dirSnapshot),
		uploadSem:  make(chan struct{}, maxParallelUploads),
		inflight:   make(map[string]struct{}),
	}
}

type dirSnapshot struct {
	loaded  time.Time
	entries []fuse.Dirent
	byName  map[string]fs.Node
	warm    []FileRef
}

func (f *driveFS) Root() (fs.Node, error) {
	return &driveDir{fsys: f, id: rootDirectoryID}, nil
}

func (f *driveFS) snapshot(id string) (*dirSnapshot, error) {
	f.mu.Lock()
	snap := f.dirs[id]
	f.mu.Unlock()
	if snap != nil && time.Since(snap.loaded) < dirSnapshotTTL {
		return snap, nil
	}
	v, err, _ := f.sf.Do(id, func() (interface{}, error) {
		s, err := f.loadSnapshot(id)
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		f.dirs[id] = s
		if len(f.dirs) > 512 {
			for k, old := range f.dirs {
				if time.Since(old.loaded) > dirSnapshotTTL {
					delete(f.dirs, k)
				}
			}
		}
		f.mu.Unlock()
		return s, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*dirSnapshot), nil
}

func (f *driveFS) loadSnapshot(id string) (*dirSnapshot, error) {
	defer traceSlow("loadSnapshot", id, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := f.db.QueryContext(ctx, `
		SELECT id, name, mime_type, size, modified_time
		FROM files
		WHERE parent_id = ?
		ORDER BY name, id
	`, id)
	if err != nil {
		return nil, fmt.Errorf("consultar arquivos do diretório %q: %w", id, err)
	}
	defer rows.Close()

	snap := &dirSnapshot{loaded: time.Now(), byName: make(map[string]fs.Node)}

	for rows.Next() {
		var fid string
		var name, mime sql.NullString
		var size sql.NullInt64
		var mtime sql.NullTime
		if err := rows.Scan(&fid, &name, &mime, &size, &mtime); err != nil {
			return nil, fmt.Errorf("ler entrada do diretório %q: %w", id, err)
		}

		display := safeName(name.String)
		if display == "" {
			continue
		}

		// Ignora completamente os arquivos nativos do Google Workspace
		if isGoogleApp(mime.String) {
			continue
		}

		display = uniqueName(snap.byName, display, fid)

		entry := fuse.Dirent{Inode: fs.GenerateDynamicInode(1, fid), Name: display}
		if mime.String == driveFolderMIMEType {
			entry.Type = fuse.DT_Dir
			snap.byName[display] = &driveDir{fsys: f, id: fid}
		} else {
			entry.Type = fuse.DT_File
			file := &driveFile{fsys: f, id: fid, name: display, mimeType: mime.String, size: size.Int64, parent: id}

			if mtime.Valid {
				file.mtime = mtime.Time
			}
			snap.byName[display] = file
			if file.size > 0 && len(snap.warm) < warmMaxFiles {
				snap.warm = append(snap.warm, file.ref())
			}
		}
		snap.entries = append(snap.entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterar diretório %q: %w", id, err)
	}
	return snap, nil
}

func (f *driveFS) invalidateDir(id string) {
	f.mu.Lock()
	delete(f.dirs, id)
	f.mu.Unlock()
}

func (f *driveFS) invalidateDirs(ids map[string]struct{}) {
	if len(ids) == 0 {
		return
	}
	f.mu.Lock()
	for id := range ids {
		delete(f.dirs, id)
	}
	f.mu.Unlock()
}

func (f *driveFS) invalidateAll() {
	f.mu.Lock()
	f.dirs = make(map[string]*dirSnapshot)
	f.mu.Unlock()
}

func (f *driveFS) insertLocal(id, name, mime, parent string, size int64) error {
	_, err := f.db.Exec(`INSERT OR REPLACE INTO files
		(id, name, mime_type, parent_id, size, modified_time)
		VALUES (?, ?, ?, ?, ?, ?)`,
		id, name, mime, parent, size, time.Now().UTC())
	return err
}

func (f *driveFS) nextLocalID() string {
	return fmt.Sprintf("%s%d", localIDPrefix, time.Now().UnixNano())
}

// ---- Uploads -----------------------------------------------------------------

func uploadTimeout(size int64) time.Duration {
	d := 2*time.Minute + time.Duration(size/(256*1024))*time.Second // piso de ~256 KiB/s
	if d > 6*time.Hour {
		d = 6 * time.Hour
	}
	return d
}

// enqueueUpload envia um arquivo do staging em segundo plano. O staging e o registro em
// pending_uploads só são apagados DEPOIS de o Drive confirmar; em qualquer falha ficam
// onde estão e uploadRetryLoop tenta de novo (inclusive após reiniciar o app).
func (f *driveFS) enqueueUpload(p pendingUpload) {
	f.upMu.Lock()
	if _, busy := f.inflight[p.StagingPath]; busy {
		f.upMu.Unlock()
		return
	}
	f.inflight[p.StagingPath] = struct{}{}
	f.upMu.Unlock()

	f.uploadWG.Add(1)
	go func() {
		defer f.uploadWG.Done()
		defer func() {
			f.upMu.Lock()
			delete(f.inflight, p.StagingPath)
			f.upMu.Unlock()
		}()

		f.uploadSem <- struct{}{}
		defer func() { <-f.uploadSem }()

		if err := f.doUpload(p); err != nil {
			log.Printf("Erro ao enviar %q para o Drive: %v. Mantido no staging (%s); nova tentativa em até %v.",
				p.Name, err, p.StagingPath, uploadRetryInterval)
		}
	}()
}

func (f *driveFS) doUpload(p pendingUpload) error {
	file, err := os.Open(p.StagingPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Não há mais o que enviar; descarta o registro para não tentar para sempre.
			log.Printf("Aviso: staging de %q desapareceu (%s); descartando o registro pendente.", p.Name, p.StagingPath)
			return RemovePendingUpload(f.db, p.StagingPath)
		}
		return fmt.Errorf("abrir staging: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("consultar staging: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout(info.Size()))
	defer cancel()

	var uploaded *drive.File
	if p.IsNew {
		uploaded, err = f.srv.Files.Create(&drive.File{
			Name: p.Name, MimeType: p.MimeType, Parents: []string{p.ParentID},
		}).Media(file).Context(ctx).Fields("id,name,mimeType,size,modifiedTime").Do()
	} else {
		uploaded, err = f.srv.Files.Update(p.FileID, &drive.File{}).
			Media(file).Context(ctx).Fields("id,size,modifiedTime").Do()
	}
	if err != nil {
		return err
	}

	mtime := time.Now().UTC()
	if uploaded.ModifiedTime != "" {
		if parsed, parseErr := time.Parse(time.RFC3339, uploaded.ModifiedTime); parseErr == nil {
			mtime = parsed
		}
	}
	size := info.Size()
	if uploaded.Size > 0 || size == 0 {
		size = uploaded.Size
	}

	// Confirmação local numa única transação (troca de ID + remoção do registro pendente).
	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("confirmar upload no cache local: %w", err)
	}
	defer tx.Rollback()
	if p.IsNew {
		// O polling pode já ter inserido a linha com o ID real; evita violar a PK.
		if _, err := tx.Exec("DELETE FROM files WHERE id = ?", uploaded.Id); err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE files SET id = ?, size = ?, modified_time = ? WHERE id = ?`,
			uploaded.Id, size, mtime, p.FileID)
	} else {
		_, err = tx.Exec(`UPDATE files SET size = ?, modified_time = ? WHERE id = ?`,
			size, mtime, p.FileID)
	}
	if err != nil {
		return fmt.Errorf("atualizar cache local: %w", err)
	}
	if err := RemovePendingUpload(tx, p.StagingPath); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("confirmar upload no cache local: %w", err)
	}

	_ = os.Remove(p.StagingPath) // só agora o staging pode sumir
	f.invalidateDir(p.ParentID)
	return nil
}

// resumePendingUploads enfileira tudo o que está registrado como pendente.
func (f *driveFS) resumePendingUploads() {
	items, err := ListPendingUploads(f.db)
	if err != nil {
		log.Printf("Aviso: %v", err)
		return
	}
	for _, p := range items {
		f.enqueueUpload(p)
	}
}

// uploadRetryLoop retoma os uploads pendentes na partida e depois periodicamente.
func (f *driveFS) uploadRetryLoop(ctx context.Context) {
	f.logOrphanStaging()
	f.resumePendingUploads()

	ticker := time.NewTicker(uploadRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.resumePendingUploads()
		}
	}
}

// logOrphanStaging avisa (sem apagar) sobre arquivos no staging sem registro pendente,
// por exemplo restos de versões anteriores do app.
func (f *driveFS) logOrphanStaging() {
	entries, err := os.ReadDir(f.stagingDir)
	if err != nil {
		return
	}
	items, err := ListPendingUploads(f.db)
	if err != nil {
		return
	}
	known := make(map[string]struct{}, len(items))
	for _, p := range items {
		known[p.StagingPath] = struct{}{}
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(f.stagingDir, e.Name())
		if _, ok := known[path]; !ok {
			log.Printf("Aviso: arquivo órfão no staging (mantido, nada foi apagado): %s", path)
		}
	}
}

// WaitUploads espera os envios em andamento; false se estourou o prazo.
func (f *driveFS) WaitUploads(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		f.uploadWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// fillStaging copia o arquivo do Drive para o staging em pedaços, sem carregá-lo inteiro na RAM.
func (f *driveFS) fillStaging(ctx context.Context, ref FileRef, dst *os.File) error {
	const chunk = int64(4 << 20)
	for off := int64(0); off < ref.Size; off += chunk {
		n := int(min64(chunk, ref.Size-off))
		data, err := f.cache.ReadRange(ctx, ref, off, n)
		if err != nil {
			return err
		}
		if _, err := dst.WriteAt(data, off); err != nil {
			return err
		}
	}
	return nil
}

// ---- Diretórios --------------------------------------------------------------

type driveDir struct {
	fsys *driveFS
	id   string
}

func (d *driveDir) Attr(_ context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0755
	attr.Inode = fs.GenerateDynamicInode(1, d.id)
	attr.Valid = 1 * time.Hour
	return nil
}

func (d *driveDir) ReadDirAll(_ context.Context) ([]fuse.Dirent, error) {
	defer traceSlow("ReadDirAll", d.id, time.Now())
	snap, err := d.fsys.snapshot(d.id)
	if err != nil {
		return nil, err
	}
	d.fsys.cache.WarmHeads(snap.warm)
	return snap.entries, nil
}

func (d *driveDir) Lookup(_ context.Context, name string) (fs.Node, error) {
	defer traceSlow("Lookup", name, time.Now())
	snap, err := d.fsys.snapshot(d.id)
	if err != nil {
		return nil, err
	}
	if n, ok := snap.byName[name]; ok {
		return n, nil
	}
	return nil, fuse.ENOENT
}

// Mkdir cria a pasta no Drive de forma SÍNCRONA e já devolve o ID real. Pastas nunca
// têm ID provisório, então criar itens dentro de uma pasta nova funciona de imediato.
func (d *driveDir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	name := safeName(req.Name)
	if name == "" {
		return nil, syscall.EINVAL
	}
	snap, err := d.fsys.snapshot(d.id)
	if err != nil {
		return nil, err
	}
	if _, exists := snap.byName[name]; exists {
		return nil, fuse.EEXIST
	}

	apiCtx, cancel := context.WithTimeout(ctx, apiOpTimeout)
	defer cancel()
	created, err := d.fsys.srv.Files.Create(&drive.File{
		Name: name, MimeType: driveFolderMIMEType, Parents: []string{d.id},
	}).Fields("id").Context(apiCtx).Do()
	if err != nil {
		log.Printf("Erro ao criar pasta %q no Drive: %v", name, err)
		return nil, fuse.EIO
	}
	if err := d.fsys.insertLocal(created.Id, name, driveFolderMIMEType, d.id, 0); err != nil {
		return nil, fmt.Errorf("registrar pasta localmente: %w", err)
	}
	d.fsys.invalidateDir(d.id)
	return &driveDir{fsys: d.fsys, id: created.Id}, nil
}

func (d *driveDir) Create(_ context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	name := safeName(req.Name)
	if name == "" {
		return nil, nil, syscall.EINVAL
	}
	snap, err := d.fsys.snapshot(d.id)
	if err != nil {
		return nil, nil, err
	}
	if _, exists := snap.byName[name]; exists {
		return nil, nil, fuse.EEXIST
	}
	localID := d.fsys.nextLocalID()
	if err := d.fsys.insertLocal(localID, name, "application/octet-stream", d.id, 0); err != nil {
		return nil, nil, fmt.Errorf("criar arquivo localmente: %w", err)
	}
	if err := os.MkdirAll(d.fsys.stagingDir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("criar staging: %w", err)
	}
	path := filepath.Join(d.fsys.stagingDir, localID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("abrir staging: %w", err)
	}
	d.fsys.invalidateDir(d.id)
	node := &driveFile{fsys: d.fsys, id: localID, parent: d.id, name: name, mimeType: "application/octet-stream"}
	h := &fileHandle{cache: d.fsys.cache, ref: node.ref(), staging: file, writable: true,
		dirty: true, // arquivo novo sempre é criado no Drive, mesmo vazio
		fsys:  d.fsys, parent: d.id, name: name, mimeType: node.mimeType,
		fileID: localID, stagingPath: path, isNew: true}
	resp.Flags |= fuse.OpenKeepCache
	return node, h, nil
}

func (d *driveDir) Remove(_ context.Context, req *fuse.RemoveRequest) error {
	snap, err := d.fsys.snapshot(d.id)
	if err != nil {
		return err
	}
	node, ok := snap.byName[req.Name]
	if !ok {
		return fuse.ENOENT
	}
	var id string
	switch n := node.(type) {
	case *driveFile:
		id = n.id
	case *driveDir:
		id = n.id
	default:
		return fuse.EIO
	}
	if _, err := d.fsys.db.Exec("DELETE FROM files WHERE id = ?", id); err != nil {
		return fmt.Errorf("remover entrada localmente: %w", err)
	}
	d.fsys.invalidateDir(d.id)
	name := req.Name
	d.fsys.uploadWG.Add(1)
	go func() {
		defer d.fsys.uploadWG.Done()
		ctx, cancel := context.WithTimeout(context.Background(), apiOpTimeout)
		defer cancel()
		if _, err := d.fsys.srv.Files.Update(id, &drive.File{Trashed: true}).Fields("id").Context(ctx).Do(); err != nil {
			log.Printf("Erro ao mover %q para a lixeira do Drive: %v", name, err)
		}
	}()
	return nil
}

func (d *driveDir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	targetDir, ok := newDir.(*driveDir)
	if !ok || targetDir == nil {
		return syscall.EINVAL
	}

	var targetID string
	err := d.fsys.db.QueryRowContext(ctx, `
		SELECT id
		FROM files
		WHERE parent_id = ? AND name = ?
		LIMIT 1
	`, d.id, req.OldName).Scan(&targetID)
	if errors.Is(err, sql.ErrNoRows) {
		return fuse.ENOENT
	}
	if err != nil {
		return fmt.Errorf("buscar entrada %q para renomear: %w", req.OldName, err)
	}

	if _, err := d.fsys.db.Exec(`
		UPDATE files
		SET name = ?, parent_id = ?
		WHERE id = ?
	`, req.NewName, targetDir.id, targetID); err != nil {
		return fmt.Errorf("atualizar entrada %q localmente: %w", req.OldName, err)
	}

	d.fsys.invalidateDir(d.id)
	if targetDir.id != d.id {
		d.fsys.invalidateDir(targetDir.id)
	}

	oldName := req.OldName
	newName := req.NewName
	sourceDirID := d.id
	targetDirID := targetDir.id
	d.fsys.uploadWG.Add(1)
	go func() {
		defer d.fsys.uploadWG.Done()
		apiCtx, cancel := context.WithTimeout(context.Background(), apiOpTimeout)
		defer cancel()
		call := d.fsys.srv.Files.Update(targetID, &drive.File{Name: newName}).Context(apiCtx)
		if sourceDirID != targetDirID {
			call = call.AddParents(targetDirID).RemoveParents(sourceDirID)
		}
		if _, err := call.Do(); err != nil {
			log.Printf("Erro ao renomear/mover %q para %q no Drive: %v", oldName, newName, err)
		}
	}()

	return nil
}

// ---- Arquivos ----------------------------------------------------------------

type driveFile struct {
	fsys     *driveFS
	id       string
	parent   string
	name     string
	mimeType string
	size     int64
	mtime    time.Time
}

func (f *driveFile) ref() FileRef {
	return FileRef{ID: f.id, Size: f.size, MTime: f.mtime.Unix()}
}

func (f *driveFile) Attr(_ context.Context, attr *fuse.Attr) error {
	attr.Mode = 0644
	attr.Inode = fs.GenerateDynamicInode(1, f.id)
	attr.Valid = 1 * time.Minute
	attr.Size = uint64(f.size)
	if !f.mtime.IsZero() {
		attr.Mtime = f.mtime
	}
	return nil
}

func (f *driveFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags |= fuse.OpenKeepCache
	if req.Flags&fuse.OpenAccessModeMask == fuse.OpenReadOnly {
		return &fileHandle{cache: f.fsys.cache, ref: f.ref()}, nil
	}
	if err := os.MkdirAll(f.fsys.stagingDir, 0o700); err != nil {
		return nil, fmt.Errorf("criar staging: %w", err)
	}
	path := filepath.Join(f.fsys.stagingDir, f.id+"-"+fmt.Sprint(time.Now().UnixNano()))
	staging, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("abrir staging: %w", err)
	}
	truncate := req.Flags&fuse.OpenTruncate != 0
	if !truncate && f.size > 0 {
		if err := f.fsys.fillStaging(ctx, f.ref(), staging); err != nil {
			staging.Close()
			os.Remove(path)
			return nil, fmt.Errorf("preparar edição de %q: %w", f.name, err)
		}
	}
	return &fileHandle{
		cache: f.fsys.cache, ref: f.ref(), staging: staging, stagingPath: path,
		writable: true, dirty: truncate, fsys: f.fsys, parent: f.parent,
		name: f.name, mimeType: f.mimeType, fileID: f.id, isNew: false,
	}, nil
}

type fileHandle struct {
	cache *FileCache
	ref   FileRef

	mu                             sync.Mutex
	hi                             int64
	seq                            int
	dirty                          bool // houve escrita/truncamento: só então vale enviar
	staging                        *os.File
	stagingPath                    string
	writable                       bool
	isNew                          bool
	fsys                           *driveFS
	parent, name, mimeType, fileID string
	once                           sync.Once
}

func (h *fileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	defer traceSlow("Read", h.ref.ID, time.Now())
	if h.staging != nil {
		buf := make([]byte, req.Size)
		n, err := h.staging.ReadAt(buf, req.Offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return fuse.EIO
		}
		resp.Data = buf[:n]
		return nil
	}
	data, err := h.cache.ReadRange(ctx, h.ref, req.Offset, req.Size)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return fuse.EINTR
		}
		log.Printf("Erro ao ler %s@%d+%d: %v", h.ref.ID, req.Offset, req.Size, err)
		return fuse.EIO
	}
	resp.Data = data
	h.readAhead(req.Offset, int64(len(data)))
	return nil
}

func (h *fileHandle) Write(_ context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if !h.writable || h.staging == nil {
		return fuse.EPERM
	}
	n, err := h.staging.WriteAt(req.Data, req.Offset)
	if err != nil {
		return fuse.EIO
	}
	h.mu.Lock()
	h.dirty = true
	h.mu.Unlock()
	resp.Size = n
	return nil
}

func (h *fileHandle) readAhead(off, n int64) {
	if n <= 0 {
		return
	}
	end := off + n
	h.mu.Lock()
	if off >= h.hi-blockSize && off <= h.hi+blockSize/2 {
		h.seq++
	} else {
		h.seq = 0
	}
	if end > h.hi {
		h.hi = end
	}
	seq := h.seq
	h.mu.Unlock()
	if seq < 3 {
		return
	}
	last := blockIndexFor(end - 1)
	for i := int64(1); i <= readAheadBlocks; i++ {
		h.cache.Prefetch(h.ref, last+i)
	}
}

func (h *fileHandle) Flush(_ context.Context, _ *fuse.FlushRequest) error { return nil }

func (h *fileHandle) Release(_ context.Context, _ *fuse.ReleaseRequest) error {
	var closeErr error
	h.once.Do(func() {
		if h.staging == nil {
			return // handle somente leitura
		}
		h.mu.Lock()
		dirty := h.dirty
		h.mu.Unlock()

		// Sync garante que o conteúdo está em disco antes de contarmos com o staging.
		if err := h.staging.Sync(); err != nil {
			closeErr = err
		}
		if err := h.staging.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		if closeErr != nil {
			log.Printf("Erro ao finalizar staging de %q (%s): %v. Arquivo mantido, nada foi enviado.",
				h.name, h.stagingPath, closeErr)
			return
		}
		if !dirty {
			_ = os.Remove(h.stagingPath) // aberto para escrita mas nada mudou
			return
		}

		p := pendingUpload{
			StagingPath: h.stagingPath, FileID: h.fileID, Name: h.name,
			MimeType: h.mimeType, ParentID: h.parent, IsNew: h.isNew,
		}
		if err := AddPendingUpload(h.fsys.db, p); err != nil {
			// Ainda tentamos enviar agora; só perdemos a retomada após reinício.
			log.Printf("Aviso: %v", err)
		}
		h.fsys.enqueueUpload(p)
	})
	return closeErr
}

// ---- Montagem ----------------------------------------------------------------

// unmountFS desmonta o ponto de montagem; se estiver ocupado, cai para desmontagem preguiçosa.
func unmountFS(mountPoint string) {
	if err := fuse.Unmount(mountPoint); err == nil {
		return
	}
	if err := exec.Command("fusermount", "-u", "-z", mountPoint).Run(); err != nil {
		log.Printf("Aviso: falha ao desmontar %q: %v", mountPoint, err)
	}
}

// MountGoogleDrive monta o FS e só retorna depois que o servidor FUSE parar. Cancelar
// ctx desmonta (é assim que o onExit encerra o motor).
func MountGoogleDrive(ctx context.Context, db *sql.DB, srv *drive.Service, paths appPaths) error {
	mountPoint := paths.mountPoint

	cache, err := NewFileCache(srv, paths.blocksDir, MaxDiskCacheBytes)
	if err != nil {
		return err
	}

	_ = exec.Command("fusermount", "-u", "-z", mountPoint).Run()

	if err := os.MkdirAll(mountPoint, 0o700); err != nil {
		return fmt.Errorf("criar ponto de montagem %q: %w", mountPoint, err)
	}
	info, err := os.Stat(mountPoint)
	if err != nil {
		return fmt.Errorf("verificar ponto de montagem %q: %w", mountPoint, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("ponto de montagem %q não é um diretório", mountPoint)
	}

	conn, err := fuse.Mount(
		mountPoint,
		fuse.FSName("meu-gdrive-client"),
		fuse.Subtype("googledrive"),
		fuse.AsyncRead(),
		fuse.MaxReadahead(1<<20),
	)
	if err != nil {
		return fmt.Errorf("montar FUSE em %q: %w", mountPoint, err)
	}
	defer conn.Close()

	fsys := newDriveFS(db, cache, srv, paths.stagingDir)
	runtimeState.mu.Lock()
	runtimeState.fsys = fsys
	runtimeState.mu.Unlock()

	go func() {
		<-ctx.Done()
		unmountFS(mountPoint)
	}()
	go fsys.WatchRemoteChanges(ctx)
	go fsys.uploadRetryLoop(ctx)

	if err := fs.Serve(conn, fsys); err != nil {
		return fmt.Errorf("servir sistema de arquivos FUSE: %w", err)
	}
	return nil
}

var _ fs.FS = (*driveFS)(nil)
var _ fs.Node = (*driveDir)(nil)
var _ fs.HandleReadDirAller = (*driveDir)(nil)
var _ fs.NodeStringLookuper = (*driveDir)(nil)
var _ fs.NodeMkdirer = (*driveDir)(nil)
var _ fs.NodeCreater = (*driveDir)(nil)
var _ fs.NodeRemover = (*driveDir)(nil)
var _ fs.NodeRenamer = (*driveDir)(nil)
var _ fs.Node = (*driveFile)(nil)
var _ fs.NodeOpener = (*driveFile)(nil)
var _ fs.HandleReader = (*fileHandle)(nil)
var _ fs.HandleWriter = (*fileHandle)(nil)
var _ fs.HandleFlusher = (*fileHandle)(nil)
var _ fs.HandleReleaser = (*fileHandle)(nil)
