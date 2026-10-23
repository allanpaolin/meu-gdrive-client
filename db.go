package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

const (
	syncKeyChangesToken = "changes_page_token"
	syncKeyInitialDone  = "initial_sync_done"

	// Prefixo dos IDs provisórios de arquivos criados localmente e ainda não enviados.
	localIDPrefix = "local-"
)

// dbExecer é satisfeito tanto por *sql.DB quanto por *sql.Tx.
type dbExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

// pendingUpload descreve um arquivo no staging que ainda precisa ir para o Drive.
// Fica persistido no SQLite para sobreviver a falhas de rede, crashes e reinícios.
type pendingUpload struct {
	StagingPath string
	FileID      string
	Name        string
	MimeType    string
	ParentID    string
	IsNew       bool
}

// InitDB abre (ou cria) o banco de metadados. O arquivo é criado com 0600 ANTES de o
// SQLite tocar nele; os arquivos -wal/-shm herdam essas permissões.
func InitDB(dbPath string) (*sql.DB, error) {
	f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("criar banco %q: %w", dbPath, err)
	}
	_ = f.Close()
	if err := os.Chmod(dbPath, 0o600); err != nil {
		return nil, fmt.Errorf("ajustar permissões do banco %q: %w", dbPath, err)
	}

	// Os PRAGMAs vão no DSN: são por conexão e o database/sql abre várias conexões.
	dsn := dbPath + "?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("abrir banco SQLite: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("conectar ao banco SQLite: %w", err)
	}

	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS files (
			id TEXT PRIMARY KEY,
			name TEXT,
			mime_type TEXT,
			parent_id TEXT,
			size INTEGER,
			modified_time DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS sync_state (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS pending_uploads (
			staging_path TEXT PRIMARY KEY,
			file_id      TEXT NOT NULL,
			name         TEXT NOT NULL,
			mime_type    TEXT NOT NULL,
			parent_id    TEXT NOT NULL,
			is_new       INTEGER NOT NULL,
			created_at   INTEGER NOT NULL
		)`,
		"CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent_id)",
		"CREATE INDEX IF NOT EXISTS idx_files_parent_name ON files(parent_id, name)",
	} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("preparar esquema SQLite: %w", err)
		}
	}

	// Bancos criados por versões anteriores podem ter -wal/-shm com 0644.
	for _, suffix := range []string{"-wal", "-shm"} {
		_ = os.Chmod(dbPath+suffix, 0o600)
	}

	log.Println("Banco de dados SQLite (cache local) pronto.")
	return db, nil
}

func GetSyncState(db dbExecer, key string) (string, error) {
	var value string
	err := db.QueryRow("SELECT value FROM sync_state WHERE key = ?", key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("ler estado de sincronização %q: %w", key, err)
	}
	return value, nil
}

// SetSyncState aceita *sql.DB ou *sql.Tx, para poder entrar em transações maiores.
func SetSyncState(db dbExecer, key, value string) error {
	_, err := db.Exec(`
		INSERT INTO sync_state(key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, value)
	if err != nil {
		return fmt.Errorf("salvar estado de sincronização %q: %w", key, err)
	}
	return nil
}

// InitialSyncDone informa se a varredura completa do Drive terminou com sucesso.
// O marcador só é gravado no FINAL de SyncMetadata, na mesma transação do PageToken.
func InitialSyncDone(db *sql.DB) (bool, error) {
	marker, err := GetSyncState(db, syncKeyInitialDone)
	if err != nil {
		return false, err
	}
	if marker == "1" {
		return true, nil
	}

	// Migração: versões antigas só salvavam o PageToken depois de a varredura
	// terminar. Banco com linhas + token = varredura antiga completa.
	token, err := GetSyncState(db, syncKeyChangesToken)
	if err != nil {
		return false, err
	}
	if token == "" {
		return false, nil
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM files").Scan(&count); err != nil {
		return false, fmt.Errorf("contar metadados locais: %w", err)
	}
	if count == 0 {
		return false, nil
	}
	if err := SetSyncState(db, syncKeyInitialDone, "1"); err != nil {
		return false, err
	}
	return true, nil
}

// ---- Uploads pendentes -------------------------------------------------------

func AddPendingUpload(db dbExecer, p pendingUpload) error {
	isNew := 0
	if p.IsNew {
		isNew = 1
	}
	_, err := db.Exec(`
		INSERT OR REPLACE INTO pending_uploads
			(staging_path, file_id, name, mime_type, parent_id, is_new, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, p.StagingPath, p.FileID, p.Name, p.MimeType, p.ParentID, isNew, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("registrar upload pendente de %q: %w", p.Name, err)
	}
	return nil
}

func RemovePendingUpload(db dbExecer, stagingPath string) error {
	_, err := db.Exec("DELETE FROM pending_uploads WHERE staging_path = ?", stagingPath)
	if err != nil {
		return fmt.Errorf("remover upload pendente: %w", err)
	}
	return nil
}

func ListPendingUploads(db *sql.DB) ([]pendingUpload, error) {
	rows, err := db.Query(`
		SELECT staging_path, file_id, name, mime_type, parent_id, is_new
		FROM pending_uploads
		ORDER BY created_at, rowid
	`)
	if err != nil {
		return nil, fmt.Errorf("listar uploads pendentes: %w", err)
	}
	defer rows.Close()

	var out []pendingUpload
	for rows.Next() {
		var p pendingUpload
		var isNew int
		if err := rows.Scan(&p.StagingPath, &p.FileID, &p.Name, &p.MimeType, &p.ParentID, &isNew); err != nil {
			return nil, fmt.Errorf("ler upload pendente: %w", err)
		}
		p.IsNew = isNew != 0
		out = append(out, p)
	}
	return out, rows.Err()
}

// ---- Sincronização completa --------------------------------------------------

// SyncMetadata faz a varredura completa do Drive e troca o mapa local de forma
// atômica. Serve tanto para a primeira sincronização quanto para a recuperação quando
// o token de mudanças fica inválido. Em caso de erro o mapa anterior permanece intacto.
func SyncMetadata(ctx context.Context, srv *drive.Service, db *sql.DB) error {
	log.Println("Iniciando varredura do Google Drive para o cache local...")

	const maxAttempts = 3
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = syncMetadataOnce(ctx, srv, db)
		if err == nil {
			log.Println("Sincronização de metadados concluída.")
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isTransientError(err) || attempt == maxAttempts {
			break
		}
		log.Printf("Aviso: erro temporário na sincronização (tentativa %d/%d): %v. Nova tentativa em instantes.", attempt, maxAttempts, err)
		select {
		case <-time.After(time.Duration(attempt) * 2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("sincronizar metadados: %w", err)
}

func syncMetadataOnce(ctx context.Context, srv *drive.Service, db *sql.DB) error {
	// 1) O StartPageToken é obtido ANTES da varredura: qualquer mudança feita no Drive
	//    enquanto a varredura roda será reaplicada pelo polling (as operações são
	//    idempotentes). Se o token fosse pego depois, essas mudanças se perderiam.
	startToken, err := srv.Changes.GetStartPageToken().Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("obter StartPageToken: %w", err)
	}
	root, err := srv.Files.Get("root").Fields("id").Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("obter ID da raiz do Meu Drive: %w", err)
	}
	rootID := root.Id

	// 2) A varredura escreve numa tabela temporária; o mapa em uso não é tocado.
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS files_scan"); err != nil {
		return fmt.Errorf("limpar tabela de varredura: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE files_scan (
		id TEXT PRIMARY KEY,
		name TEXT,
		mime_type TEXT,
		parent_id TEXT,
		size INTEGER,
		modified_time DATETIME
	)`); err != nil {
		return fmt.Errorf("criar tabela de varredura: %w", err)
	}
	cleanup := func() { _, _ = db.Exec("DROP TABLE IF EXISTS files_scan") }

	call := srv.Files.List().
		Fields("nextPageToken, files(id, name, mimeType, parents, size, modifiedTime)").
		Q("trashed = false").
		PageSize(1000)

	total := 0
	err = call.Pages(ctx, func(page *drive.FileList) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("iniciar transação de metadados: %w", err)
		}
		defer tx.Rollback()
		stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO files_scan
			(id, name, mime_type, parent_id, size, modified_time) VALUES (?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("preparar inserção de metadados: %w", err)
		}
		defer stmt.Close()

		for _, f := range page.Files {
			parent := "shared_with_me"
			if len(f.Parents) > 0 {
				parent = f.Parents[0]
				if parent == rootID || parent == "root" {
					parent = rootDirectoryID
				}
			}
			if _, err := stmt.ExecContext(ctx, f.Id, f.Name, f.MimeType, parent, f.Size, f.ModifiedTime); err != nil {
				return fmt.Errorf("inserir arquivo %q: %w", f.Name, err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("confirmar metadados da página: %w", err)
		}
		total += len(page.Files)
		log.Printf("Varredura: %d itens lidos...", total)
		return nil
	})
	if err != nil {
		cleanup()
		return err
	}

	// 3) Troca atômica: mapa novo + PageToken + marcador de sucesso na MESMA transação.
	//    Só aqui o marcador existe. Se algo falhar antes, a próxima inicialização refaz tudo.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		cleanup()
		return fmt.Errorf("iniciar troca do mapa: %w", err)
	}
	defer tx.Rollback()

	// Linhas "local-*" (arquivos criados aqui e ainda não enviados) são preservadas.
	if _, err := tx.Exec("DELETE FROM files WHERE id NOT LIKE ?", localIDPrefix+"%"); err != nil {
		_ = tx.Rollback() // libera o lock de escrita antes do DROP
		cleanup()
		return fmt.Errorf("limpar mapa antigo: %w", err)
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO files (id, name, mime_type, parent_id, size, modified_time)
		SELECT id, name, mime_type, parent_id, size, modified_time FROM files_scan`); err != nil {
		_ = tx.Rollback() // libera o lock de escrita antes do DROP
		cleanup()
		return fmt.Errorf("aplicar mapa novo: %w", err)
	}
	if _, err := tx.Exec("DROP TABLE files_scan"); err != nil {
		_ = tx.Rollback() // libera o lock de escrita antes do DROP
		cleanup()
		return fmt.Errorf("remover tabela de varredura: %w", err)
	}
	if err := SetSyncState(tx, syncKeyChangesToken, startToken.StartPageToken); err != nil {
		_ = tx.Rollback() // libera o lock de escrita antes do DROP
		cleanup()
		return err
	}
	if err := SetSyncState(tx, syncKeyInitialDone, "1"); err != nil {
		_ = tx.Rollback() // libera o lock de escrita antes do DROP
		cleanup()
		return err
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback() // libera o lock de escrita antes do DROP
		cleanup()
		return fmt.Errorf("confirmar troca do mapa: %w", err)
	}
	return nil
}

// ---- Classificação de erros --------------------------------------------------

func isRetryableDriveError(err error) bool {
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return false
	}

	switch apiErr.Code {
	case 429, 500, 502, 503, 504:
		return true
	case 403:
		for _, detail := range apiErr.Errors {
			if detail.Reason == "rateLimitExceeded" || detail.Reason == "userRateLimitExceeded" {
				return true
			}
		}
	}
	return false
}

// isTransientError: erros de API que valem nova tentativa + falhas de rede.
func isTransientError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if isRetryableDriveError(err) {
		return true
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.ErrUnexpectedEOF)
}
