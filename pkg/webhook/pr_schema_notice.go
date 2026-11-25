package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

const prSchemaCapTitle = "Schema planning paused"

func prSchemaHoldMessage(policy api.PRSchemaCapConfig, databases []string, labelPresent bool, notice string) string {
	var message strings.Builder
	fmt.Fprintf(&message, "Planning is paused on %s. This PR changes %d databases managed by this control plane, above its limit of %d.\n\n",
		schemaCapCode(policy.ControlPlane), len(databases), policy.Limit)
	switch {
	case notice != "":
		fmt.Fprintln(&message, notice)
	case labelPresent:
		fmt.Fprintf(&message, "Split the changes into smaller PRs, or remove and re-add the %s label to acknowledge this revision. Planning all %d databases can delay other SchemaBot work.\n", schemaCapCode(policy.Label), len(databases))
	default:
		fmt.Fprintf(&message, "Split the changes into smaller PRs, or add the %s label with caution. Planning all %d databases can delay other SchemaBot work.\n", schemaCapCode(policy.Label), len(databases))
	}
	message.WriteString("\nNormal apply approvals still apply.\n\nAffected databases\n")
	for _, database := range databases {
		if message.Len() > 28_000 {
			message.WriteString("- Additional database names omitted because of the output limit.\n")
			break
		}
		fmt.Fprintf(&message, "- %s\n", schemaCapCode(database))
	}
	return message.String()
}

func schemaCapCode(text string) string {
	safe := strings.Map(func(r rune) rune {
		if r == '`' || unicode.IsControl(r) {
			return '�'
		}
		return r
	}, text)
	return "`" + safe + "`"
}

func (h *Handler) publishPRSchemaHold(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, info *ghclient.PullRequestInfo, policy api.PRSchemaCapConfig, databases []string, notice string) error {
	ctx = withExpectedPRTarget(ctx, repo, pr, info)
	message := prSchemaHoldMessage(policy, databases, policy.Label != "" && info.HasLabel(policy.Label), notice)
	if h.config().AreChecksEnabled(repo) &&
		!h.postFailingAggregatesWithBlock(ctx, client, repo, pr, info.HeadSHA, h.aggregateMessagesForAllEnvironments(message), prSchemaCapBlock) {
		return fmt.Errorf("could not publish the database limit check")
	}
	return h.updatePRSchemaCapComment(ctx, client, repo, pr, policy.ControlPlane, message, true)
}

func (h *Handler) resolvePRSchemaHold(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, policy api.PRSchemaCapConfig) {
	message := fmt.Sprintf("Planning can proceed on %s.", schemaCapCode(policy.ControlPlane))
	if err := h.updatePRSchemaCapComment(ctx, client, repo, pr, policy.ControlPlane, message, false); err != nil {
		h.logger.Warn("could not update the database limit comment", "repo", repo, "pr", pr, "error", err)
	}
}

type prSchemaNotice struct {
	CommentID   int64
	MessageHash [32]byte
	LeaseUntil  time.Time
}

func (h *Handler) updatePRSchemaCapComment(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, plane, message string, hold bool) error {
	key := schemaCapKey("comment", repo, pr, plane)
	settings := h.service.Storage().Settings()
	row, err := settings.Get(ctx, key)
	if err != nil {
		return err
	}
	var previous prSchemaNotice
	if row != nil {
		if err := json.Unmarshal([]byte(row.Value), &previous); err != nil {
			return err
		}
	}
	hash := sha256.Sum256([]byte(message))
	if (previous.CommentID == 0 && !hold) || previous.MessageHash == hash || time.Now().Before(previous.LeaseUntil) {
		return nil
	}
	claim := previous
	claim.LeaseUntil = time.Now().Add(time.Minute).UTC()
	encoded, err := json.Marshal(claim)
	if err != nil {
		return err
	}
	claimed, err := settings.CompareAndSet(ctx, key, row, string(encoded))
	if err != nil || !claimed {
		return err
	}
	body := fmt.Sprintf("<!-- %s -->\n%s", key, message)
	if claim.CommentID != 0 {
		_, err = client.EditIssueComment(ctx, repo, claim.CommentID, body)
		if ghclient.IsNotFoundError(err) {
			claim.CommentID, err = 0, nil
		}
	}
	if err == nil && claim.CommentID == 0 && hold {
		claim.CommentID, _, err = client.CreateIssueComment(ctx, repo, pr, body)
	}
	claim.LeaseUntil = time.Time{}
	if err == nil {
		claim.MessageHash = hash
	}
	result, marshalErr := json.Marshal(claim)
	if marshalErr != nil {
		return marshalErr
	}
	_, storeErr := settings.CompareAndSet(ctx, key, &storage.Setting{Key: key, Value: string(encoded)}, string(result))
	if err != nil {
		return err
	}
	return storeErr
}
