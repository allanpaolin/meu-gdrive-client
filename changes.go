package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

const (
	remoteChangesInterval = 30 * time.Second
	pollTimeout           = 5 * time.Minute  // teto para UM tick inteiro (todas as páginas)
	minResyncInterval     = 10 * time.Minute // evita ressincronizações em loop
	changesPageSize       = 1000
)

var errNoPageToken = errors.New("nenhum PageToken de mudanças salvo")

// WatchRemoteChanges mantém o mapa local alinhado com o Drive. Só mexe no SQLite e nos
// snapshots em memória; o conteúdo dos arquivos continua sob demanda no FileCache.
func (f *driveFS) WatchRemoteChanges(ctx context.Context) {
	var rootID string
	var lastResync time.Time

	poll := func() {
		if ctx.Err() != nil {
			return
		}
		if rootID == "" {
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			root, err := f.srv.Files.Get("root").Fields("id").Context(rctx).Do()
			cancel()
			if err != nil {
				log.Printf("Aviso: não foi possível obter a raiz para o polling: %v", err)
				return // tenta de novo no próximo tick (ex.: estava offline)
			}
			rootID = root.Id
		}

		pctx, cancel := context.WithTimeout(ctx, pollTimeout)
		err := f.pollRemoteChanges(pctx, rootID)
		cancel()

		switch {
		case err == nil:
		case ctx.Err() != nil:
		case isInvalidChangesToken(err):
			if time.Since(lastResync) < minResyncInterval {
				log.Printf("Aviso: token de mudanças inválido, mas já houve ressincronização recente; aguardando: %v", err)
				return
			}
			log.Printf("Token de mudanças inválido ou expirado (%v). Ressincronizando o mapa do Drive...", err)
			lastResync = time.Now()
			if err := f.resync(ctx); err != nil {
				log.Printf("Aviso: falha na ressincronização: %v", err)
			}
		default:
			log.Printf("Aviso: falha ao consultar alterações remotas: %v", err)
		}
	}

	poll() // já na partida, sem esperar o primeiro tick

	ticker := time.NewTicker(remoteChangesInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}

// pollRemoteChanges consome TODAS as páginas pendentes de uma vez. Cada página é
// aplicada em uma transação junto com o próximo token: ou avança tudo, ou nada.
func (f *driveFS) pollRemoteChanges(ctx context.Context, rootID string) error {
	token, err := GetSyncState(f.db, syncKeyChangesToken)
	if err != nil {
		return err
	}
	if token == "" {
		return errNoPageToken
	}

	for {
		list, err := f.srv.Changes.List(token).
			PageSize(changesPageSize).
			IncludeRemoved(true).
			RestrictToMyDrive(true).
			Fields("nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,mimeType,parents,size,modifiedTime,trashed))").
			Context(ctx).Do()
		if err != nil {
			return err
		}

		next := list.NextPageToken
		if next == "" {
			next = list.NewStartPageToken
		}
		if next == "" {
			return fmt.Errorf("resposta de mudanças sem token seguinte")
		}

		dirty, err := f.applyChangePage(list.Changes, rootID, next)
		if err != nil {
			return err
		}
		f.invalidateDirs(dirty)

		if list.NextPageToken == "" {
			return nil // chegou ao fim: newStartPageToken já foi salvo
		}
		token = list.NextPageToken
	}
}

func (f *driveFS) applyChangePage(changes []*drive.Change, rootID, nextToken string) (map[string]struct{}, error) {
	tx, err := f.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("iniciar transação de mudanças: %w", err)
	}
	defer tx.Rollback()

	dirty := make(map[string]struct{})
	for _, change := range changes {
		if err := applyRemoteChange(tx, change, rootID, dirty); err != nil {
			id := ""
			if change != nil {
				id = change.FileId
			}
			return nil, fmt.Errorf("aplicar alteração remota %q: %w", id, err)
		}
	}
	if err := SetSyncState(tx, syncKeyChangesToken, nextToken); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("confirmar página de mudanças: %w", err)
	}
	return dirty, nil
}

func applyRemoteChange(q dbExecer, change *drive.Change, rootID string, dirty map[string]struct{}) error {
	if change == nil || change.FileId == "" {
		return nil
	}

	var oldParentNull sql.NullString
	err := q.QueryRow("SELECT parent_id FROM files WHERE id = ?", change.FileId).Scan(&oldParentNull)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	oldParent := oldParentNull.String

	if change.Removed || change.File == nil || change.File.Trashed || isGoogleApp(change.File.MimeType) {
		if _, err := q.Exec("DELETE FROM files WHERE id = ?", change.FileId); err != nil {
			return err
		}
		if oldParent != "" {
			dirty[oldParent] = struct{}{}
		}
		return nil
	}

	file := change.File
	parent := "shared_with_me"
	if len(file.Parents) > 0 {
		parent = file.Parents[0]
		if parent == rootID || parent == "root" {
			parent = rootDirectoryID
		}
	}

	if _, err := q.Exec(`
		INSERT OR REPLACE INTO files
			(id, name, mime_type, parent_id, size, modified_time)
		VALUES (?, ?, ?, ?, ?, ?)
	`, file.Id, file.Name, file.MimeType, parent, file.Size, file.ModifiedTime); err != nil {
		return err
	}

	if oldParent != "" {
		dirty[oldParent] = struct{}{}
	}
	dirty[parent] = struct{}{}
	return nil
}

// resync refaz a varredura completa (troca atômica) e descarta os snapshots em memória.
func (f *driveFS) resync(ctx context.Context) error {
	if err := SyncMetadata(ctx, f.srv, f.db); err != nil {
		return err
	}
	f.invalidateAll()
	return nil
}

// isInvalidChangesToken: token expirado/inválido/ausente. O Drive costuma responder
// 404/410 (ou 400 para token malformado). O limite minResyncInterval impede que um 400
// por outro motivo gere varreduras completas em loop.
func isInvalidChangesToken(err error) bool {
	if errors.Is(err, errNoPageToken) {
		return true
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case 400, 404, 410:
			return true
		}
	}
	return false
}
