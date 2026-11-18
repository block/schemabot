package webhook

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

const prSchemaCapCommentMarker = "<!-- schemabot:pr-schema-cap -->"
const prSchemaCapTitle = "Schema planning paused"

func prSchemaHoldMessage(policy api.PRSchemaCapConfig, schemas []ghclient.PRSchema) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Planning is paused. This PR changes %d schemas, above the limit of %d.\n\n", len(schemas), policy.Limit)
	fmt.Fprintf(&text, "Split the changes into smaller PRs, or add the `%s` label with caution. If it is already present, remove it and add it again to acknowledge this revision. Planning all %d schemas can delay other SchemaBot work. Normal apply approvals still apply.\n\nAffected schemas\n", schemaCapDisplayText(policy.Label), len(schemas))
	for _, schema := range schemas {
		if text.Len() > 30_000 {
			text.WriteString("- Additional affected schemas omitted because of the output limit.\n")
			break
		}
		fmt.Fprintf(&text, "- `%s`", schemaCapDisplayText(schema.Database))
		if schema.Namespace != "" {
			fmt.Fprintf(&text, " / `%s`", schemaCapDisplayText(schema.Namespace))
		}
		text.WriteByte('\n')
	}
	return text.String()
}

func schemaCapDisplayText(name string) string {
	return strings.Map(func(char rune) rune {
		if unicode.IsControl(char) || char == '`' {
			return '�'
		}
		return char
	}, name)
}

func (h *Handler) publishesPRSchemaCapComment(repo string, policy api.PRSchemaCapConfig) bool {
	return !h.isAggregateParticipant(repo) && (policy.PublishComment == nil || *policy.PublishComment)
}

func (h *Handler) publishPRSchemaHold(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, policy api.PRSchemaCapConfig, revision prSchemaRevision, schemas []ghclient.PRSchema) error {
	message := prSchemaHoldMessage(policy, schemas)
	if h.service.Config().AreChecksEnabled(repo) &&
		!h.postFailingAggregatesWithBlock(ctx, client, repo, pr, revision.Head, h.aggregateMessagesForAllEnvironments(message), prSchemaCapBlock) {
		return fmt.Errorf("could not publish the schema cap check")
	}
	if h.publishesPRSchemaCapComment(repo, policy) {
		return h.updatePRSchemaCapComment(ctx, client, repo, pr, message, true)
	}
	return nil
}

func (h *Handler) updatePRSchemaCapComment(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, message string, create bool) error {
	settings := h.service.Storage().Settings()
	key := fmt.Sprintf("pr_schema_cap/comment/%s/%d", repo, pr)
	row, err := settings.Get(ctx, key)
	if err != nil || (row == nil && !create) {
		return err
	}
	body := prSchemaCapCommentMarker + "\n" + message
	if row != nil {
		id, err := strconv.ParseInt(row.Value, 10, 64)
		if err != nil {
			return fmt.Errorf("decode schema cap comment ID: %w", err)
		}
		if id > 0 {
			current, err := client.GetIssueComment(ctx, repo, id)
			if err == nil && current == body {
				return nil
			}
			if err == nil {
				return client.EditIssueComment(ctx, repo, id, body)
			}
			if !ghclient.IsNotFoundError(err) || !create {
				return err
			}
		} else if time.Now().Before(time.Unix(-id, 0)) {
			return nil // Another replica is creating the comment.
		}
	}

	lease := strconv.FormatInt(-time.Now().Add(time.Minute).Unix(), 10)
	claimed, err := settings.CompareAndSet(ctx, key, row, lease)
	if err != nil || !claimed {
		return err
	}
	id, err := client.FindOwnPRComment(ctx, repo, pr, prSchemaCapCommentMarker)
	if err == nil && id == 0 {
		id, _, err = client.CreateIssueComment(ctx, repo, pr, body)
	} else if err == nil {
		err = client.EditIssueComment(ctx, repo, id, body)
	}
	if err != nil {
		return err
	}
	_, err = settings.CompareAndSet(ctx, key, &storage.Setting{Key: key, Value: lease}, strconv.FormatInt(id, 10))
	return err
}
