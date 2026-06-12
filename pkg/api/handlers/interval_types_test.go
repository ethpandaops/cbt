package handlers

import (
	"net/http"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetIntervalTypes(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	cfg := IntervalTypesConfig{
		"slot": []IntervalTypeTransformation{
			{Name: "timestamp", Expression: "value * 12", Format: "datetime"},
		},
	}
	svc := &testutil.FakeModelsService{DAG: &testutil.FakeDAGReader{}}
	server := NewServer(svc, &adminfake.FakeAdminService{}, cfg, log)

	app := fiber.New()
	app.Get("/interval/types", func(c fiber.Ctx) error {
		return server.GetIntervalTypes(c)
	})

	resp := doGet(t, app, "/interval/types")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		IntervalTypes map[string][]IntervalTypeTransformation `json:"interval_types"`
	}
	decode(t, resp, &body)
	require.Contains(t, body.IntervalTypes, "slot")
	require.Len(t, body.IntervalTypes["slot"], 1)
	assert.Equal(t, "timestamp", body.IntervalTypes["slot"][0].Name)
}
