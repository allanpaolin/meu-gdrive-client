package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getlantern/systray"
	"github.com/zalando/go-keyring"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

const (
	appDirName     = "meu-gdrive-client"
	keyringService = "MeuGDriveClient"
	keyringUser    = "DriveOAuthToken_v3"

	loginTimeout       = 5 * time.Minute  // tempo para o usuário autorizar no navegador
	engineStopTimeout  = 15 * time.Second // espera o FUSE desmontar no encerramento
	uploadDrainTimeout = 90 * time.Second // espera os uploads em andamento no encerramento
)

// MaxDiskCacheBytes é o limite do cache de blocos (GDRIVE_CACHE_GB sobrescreve).
var MaxDiskCacheBytes int64 = DefaultCacheLimit

//go:embed icon.png
var iconData []byte

// ---- Caminhos ----------------------------------------------------------------

type appPaths struct {
	configDir       string // ~/.config/meu-gdrive-client  (credentials.json, metadata.db)
	cacheDir        string // ~/.cache/meu-gdrive-client
	blocksDir       string // <cacheDir>/blocks   (descartável; sujeito ao limite)
	stagingDir      string // <cacheDir>/staging  (dados do usuário ainda não enviados; NUNCA limpar)
	dbPath          string
	credentialsPath string
	mountPoint      string
}

func ensurePrivateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("criar %q: %w", dir, err)
	}
	// MkdirAll não altera permissões de pastas que já existiam (criadas com 0755).
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("ajustar permissões de %q: %w", dir, err)
	}
	return nil
}

func resolvePaths() (appPaths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return appPaths{}, fmt.Errorf("diretório home: %w", err)
	}
	cfgBase, err := os.UserConfigDir() // respeita XDG_CONFIG_HOME
	if err != nil {
		return appPaths{}, fmt.Errorf("diretório de configuração: %w", err)
	}
	cacheBase, err := os.UserCacheDir() // respeita XDG_CACHE_HOME
	if err != nil {
		return appPaths{}, fmt.Errorf("diretório de cache: %w", err)
	}

	p := appPaths{
		configDir:  filepath.Join(cfgBase, appDirName),
		cacheDir:   filepath.Join(cacheBase, appDirName),
		mountPoint: filepath.Join(home, "GoogleDrive"),
	}
	p.credentialsPath = filepath.Join(p.configDir, "credentials.json")
	p.dbPath = filepath.Join(p.configDir, "metadata.db")
	p.blocksDir = filepath.Join(p.cacheDir, "blocks")
	p.stagingDir = filepath.Join(p.cacheDir, "staging")

	for _, d := range []string{p.configDir, p.cacheDir, p.blocksDir, p.stagingDir} {
		if err := ensurePrivateDir(d); err != nil {
			return appPaths{}, err
		}
	}
	return p, nil
}

// ---- Estado de execução ------------------------------------------------------

type appRuntime struct {
	mu     sync.Mutex
	paths  appPaths
	db     *sql.DB
	fsys   *driveFS
	cancel context.CancelFunc
	done   chan struct{} // fechado quando o motor termina
}

var runtimeState appRuntime

// ---- Notificações ------------------------------------------------------------

// notify mostra um aviso na área de trabalho (app de bandeja não tem terminal).
func notify(title, msg string) {
	if _, err := exec.LookPath("notify-send"); err != nil {
		return
	}
	cmd := exec.Command("notify-send", "-a", "Meu GDrive", title, msg)
	go func() { _ = cmd.Run() }()
}

func openBrowser(url string) error {
	cmd := exec.Command("xdg-open", url)
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	return nil
}

// ---- Cache em disco ----------------------------------------------------------

func configureCacheLimit() {
	value := strings.TrimSpace(os.Getenv("GDRIVE_CACHE_GB"))
	if value == "" {
		return
	}
	gb, err := strconv.ParseFloat(value, 64)
	if err != nil || gb <= 0 {
		log.Printf("[Cache] GDRIVE_CACHE_GB inválido (%q); usando %.0f GB", value, float64(MaxDiskCacheBytes)/(1024*1024*1024))
		return
	}
	MaxDiskCacheBytes = int64(gb * 1024 * 1024 * 1024)
}

// EnforceCacheLimit apara o diretório de BLOCOS até ~90% do limite, apagando os mais
// antigos. Roda uma vez na partida, antes de o FileCache indexar o diretório; durante a
// execução quem mantém o limite é o LRU do próprio FileCache. Recusa qualquer diretório
// que não se chame "blocks", para nunca encostar no staging (dados ainda não enviados).
func EnforceCacheLimit(blocksDir string) error {
	if filepath.Base(blocksDir) != "blocks" {
		return fmt.Errorf("recusando limpar %q: não é o diretório de blocos", blocksDir)
	}
	entries, err := os.ReadDir(blocksDir)
	if err != nil {
		return fmt.Errorf("listar cache %q: %w", blocksDir, err)
	}

	type cacheFile struct {
		path    string
		size    int64
		modTime time.Time
	}
	var files []cacheFile
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		total += info.Size()
		files = append(files, cacheFile{filepath.Join(blocksDir, e.Name()), info.Size(), info.ModTime()})
	}
	if total <= MaxDiskCacheBytes {
		return nil
	}

	sort.Slice(files, func(i, j int) bool { return files[i].modTime.Before(files[j].modTime) })
	target := MaxDiskCacheBytes - MaxDiskCacheBytes/10
	for _, file := range files {
		if total <= target {
			break
		}
		if err := os.Remove(file.path); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				log.Printf("[Cache] Falha ao remover %q: %v", file.path, err)
			}
			continue
		}
		total -= file.size
	}
	return nil
}

// ---- Credenciais e OAuth -----------------------------------------------------

func loadOAuthConfig(path string) (*oauth2.Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("credentials.json não encontrado. Baixe-o no Google Cloud (cliente OAuth do tipo \"App para computador\") e salve em %s", path)
		}
		return nil, fmt.Errorf("ler %s: %w", path, err)
	}
	if info, err := os.Stat(path); err == nil && info.Mode().Perm()&0o077 != 0 {
		_ = os.Chmod(path, 0o600)
	}
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		return nil, fmt.Errorf("analisar credenciais: %w", err)
	}
	return config, nil
}

func getTokenFromKeyring() (*oauth2.Token, error) {
	tokenJSON, err := keyring.Get(keyringService, keyringUser)
	if err != nil {
		return nil, err
	}
	tok := &oauth2.Token{}
	if err := json.Unmarshal([]byte(tokenJSON), tok); err != nil {
		return nil, fmt.Errorf("token no keyring está corrompido: %w", err)
	}
	return tok, nil
}

func saveTokenToKeyring(tok *oauth2.Token) error {
	tokenJSON, err := json.Marshal(tok)
	if err != nil {
		return fmt.Errorf("formatar o token: %w", err)
	}
	if err := keyring.Set(keyringService, keyringUser, string(tokenJSON)); err != nil {
		return fmt.Errorf("salvar token no keyring: %w", err)
	}
	log.Println("Token salvo com segurança no keyring do sistema.")
	return nil
}

func randomString(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

const callbackPage = `<!doctype html><html lang="pt-BR"><meta charset="utf-8">
<title>Meu GDrive</title>
<body style="font-family:sans-serif;text-align:center;margin-top:20vh">
<h2>%s</h2><p>Você já pode fechar esta aba.</p></body></html>`

// loginViaBrowser faz o fluxo OAuth para app desktop: listener em 127.0.0.1 numa porta
// aleatória, `state` aleatório (anti-CSRF) e PKCE (S256). Não depende de terminal.
func loginViaBrowser(ctx context.Context, base *oauth2.Config) (*oauth2.Token, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("abrir listener local: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	cfg := *base // cópia: não mexe no RedirectURL do config original
	cfg.RedirectURL = fmt.Sprintf("http://127.0.0.1:%d/callback", port)

	state, err := randomString(32)
	if err != nil {
		listener.Close()
		return nil, fmt.Errorf("gerar state: %w", err)
	}
	verifier := oauth2.GenerateVerifier()
	authURL := cfg.AuthCodeURL(state,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("prompt", "consent"),
		oauth2.S256ChallengeOption(verifier),
	)

	type result struct {
		code string
		err  error
	}
	resCh := make(chan result, 1)
	send := func(r result) {
		select {
		case resCh <- r:
		default:
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		q := r.URL.Query()
		// Requisição sem o state correto é ruído/ataque: rejeita sem encerrar o fluxo.
		if subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(state)) != 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, callbackPage, "Requisição inválida.")
			return
		}
		if e := q.Get("error"); e != "" {
			fmt.Fprintf(w, callbackPage, "Autorização negada.")
			send(result{err: fmt.Errorf("autorização recusada: %s", e)})
			return
		}
		code := q.Get("code")
		if code == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, callbackPage, "Código ausente.")
			return
		}
		fmt.Fprintf(w, callbackPage, "Autorização concluída!")
		send(result{code: code})
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			send(result{err: err})
		}
	}()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	log.Printf("Abrindo o navegador para autorizar o acesso ao Google Drive. Se não abrir, acesse:\n%s", authURL)
	notify("Meu GDrive", "Autorize o acesso ao Google Drive na janela do navegador.")
	if err := openBrowser(authURL); err != nil {
		log.Printf("Aviso: não foi possível abrir o navegador (%v). Use o link acima.", err)
	}

	timer := time.NewTimer(loginTimeout)
	defer timer.Stop()
	var res result
	select {
	case res = <-resCh:
	case <-timer.C:
		return nil, errors.New("tempo esgotado esperando a autorização no navegador")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if res.err != nil {
		return nil, res.err
	}

	exCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tok, err := cfg.Exchange(exCtx, res.code, oauth2.VerifierOption(verifier))
	if err != nil {
		return nil, fmt.Errorf("trocar o código pelo token: %w", err)
	}
	return tok, nil
}

// savingTokenSource persiste no keyring o token renovado.
type savingTokenSource struct {
	mu   sync.Mutex
	src  oauth2.TokenSource
	last string
}

func (s *savingTokenSource) Token() (*oauth2.Token, error) {
	tok, err := s.src.Token()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if tok.AccessToken != s.last {
		s.last = tok.AccessToken
		if data, err := json.Marshal(tok); err == nil {
			if err := keyring.Set(keyringService, keyringUser, string(data)); err != nil {
				log.Printf("Aviso: não foi possível salvar o token renovado: %v", err)
			}
		}
	}
	return tok, nil
}

// getClient devolve o cliente HTTP autenticado, fazendo login só se não houver token.
func getClient(ctx context.Context, config *oauth2.Config) (*http.Client, error) {
	tok, err := getTokenFromKeyring()
	switch {
	case err == nil:
		log.Println("Token resgatado do cofre do sistema.")
	case errors.Is(err, keyring.ErrNotFound):
		log.Println("Token não encontrado no cofre. Iniciando login pelo navegador...")
		tok, err = loginViaBrowser(ctx, config)
		if err != nil {
			return nil, fmt.Errorf("login: %w", err)
		}
		if err := saveTokenToKeyring(tok); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("ler token do keyring: %w", err)
	}

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		transport = &http.Transport{}
	} else {
		transport = transport.Clone()
	}
	transport.MaxConnsPerHost = 100
	transport.MaxIdleConnsPerHost = 100

	// O oauth2 usa o http.Client do contexto como transporte base e cuida da renovação
	// do access token (expira em ~1h). Contexto de fundo: a renovação não deve morrer
	// junto com o ctx do motor.
	bg := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Transport: transport})
	src := &savingTokenSource{src: config.TokenSource(bg, tok), last: tok.AccessToken}
	return oauth2.NewClient(bg, src), nil
}

// ---- Motor -------------------------------------------------------------------

// iniciarMotorFUSE bloqueia até o FUSE ser desmontado (ctx cancelado) ou até um erro.
func iniciarMotorFUSE(ctx context.Context, paths appPaths) error {
	configureCacheLimit()

	config, err := loadOAuthConfig(paths.credentialsPath)
	if err != nil {
		return err
	}
	client, err := getClient(ctx, config)
	if err != nil {
		return err
	}
	srv, err := drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return fmt.Errorf("iniciar o serviço do Drive: %w", err)
	}
	log.Println("Conexão com o Google Drive estabelecida com segurança. Bem-vindo, Capitão!")

	db, err := InitDB(paths.dbPath)
	if err != nil {
		return err
	}
	runtimeState.mu.Lock()
	runtimeState.db = db
	runtimeState.mu.Unlock()

	done, err := InitialSyncDone(db)
	if err != nil {
		return fmt.Errorf("verificar o cache local: %w", err)
	}
	if !done {
		notify("Meu GDrive", "Primeira sincronização do Drive em andamento. Pode levar alguns minutos.")
		if err := SyncMetadata(ctx, srv, db); err != nil {
			return fmt.Errorf("sincronização inicial: %w", err)
		}
		log.Println("[Escuta Ativa] Sincronização inicial concluída; marcador e PageToken gravados.")
	} else {
		log.Println("Cache local íntegro encontrado. Pulando a sincronização completa.")
	}

	if err := EnforceCacheLimit(paths.blocksDir); err != nil {
		log.Printf("[Cache] Falha ao aplicar limite de disco: %v", err)
	}

	return MountGoogleDrive(ctx, db, srv, paths)
}

func onReady() {
	if len(iconData) > 0 {
		systray.SetIcon(iconData)
	} else {
		log.Println("[Sistema] icon.png embutido vazio; mantendo o ícone padrão da bandeja.")
	}
	systray.SetTitle("Meu GDrive")
	systray.SetTooltip("Meu GDrive")

	title := systray.AddMenuItem("Meu GDrive", "Cliente Google Drive")
	title.Disable()
	openFolder := systray.AddMenuItem("Abrir Pasta", "Abrir o Google Drive no Nemo")
	exit := systray.AddMenuItem("Sair", "Desmontar e encerrar")

	paths, err := resolvePaths()
	if err != nil {
		log.Printf("[Sistema] %v", err)
		notify("Meu GDrive", "Falha ao preparar os diretórios: "+err.Error())
		systray.Quit()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	runtimeState.mu.Lock()
	runtimeState.paths = paths
	runtimeState.cancel = cancel
	runtimeState.done = done
	runtimeState.mu.Unlock()

	go func() {
		defer close(done)
		if err := iniciarMotorFUSE(ctx, paths); err != nil && ctx.Err() == nil {
			log.Printf("[Sistema] %v", err)
			notify("Meu GDrive", err.Error())
		}
	}()

	go func() {
		for {
			select {
			case <-openFolder.ClickedCh:
				if err := openBrowser(paths.mountPoint); err != nil {
					log.Printf("[Sistema] Falha ao abrir a pasta no Nemo: %v", err)
				}
			case <-exit.ClickedCh:
				systray.Quit()
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// onExit: 1) cancela o contexto (para polling/retries e desmonta o FUSE); 2) espera o
// motor parar; 3) reenvia o que estiver pendente e espera os uploads (com prazo);
// 4) fecha o banco. Uploads que não terminarem ficam no staging e voltam na próxima partida.
func onExit() {
	runtimeState.mu.Lock()
	cancel := runtimeState.cancel
	done := runtimeState.done
	fsys := runtimeState.fsys
	db := runtimeState.db
	runtimeState.mu.Unlock()

	log.Println("[Sistema] Encerrando Meu GDrive...")
	if cancel != nil {
		cancel()
	}

	if done != nil {
		select {
		case <-done:
		case <-time.After(engineStopTimeout):
			log.Println("[Sistema] O motor não parou a tempo; seguindo com o encerramento.")
		}
	}

	if fsys != nil {
		fsys.resumePendingUploads() // uma última tentativa para o que falhou antes
		if !fsys.WaitUploads(uploadDrainTimeout) {
			msg := "Alguns arquivos ainda não foram enviados. Eles foram mantidos e serão reenviados na próxima vez que o app abrir."
			log.Println("[Sistema] " + msg)
			notify("Meu GDrive", msg)
		}
	}

	if db != nil {
		if err := db.Close(); err != nil {
			log.Printf("[Sistema] Falha ao fechar o banco SQLite: %v", err)
		}
	}
	log.Println("[Sistema] Até logo!")
}

func main() {
	// SIGINT/SIGTERM passam pelo mesmo caminho de encerramento do menu "Sair".
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-sigCtx.Done()
		systray.Quit()
	}()

	systray.Run(onReady, onExit)
}
