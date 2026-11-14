package api

import (
	"io"
	"mime"
	"net/http"
)

func (s *Server) attachment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	consumer := consumerFrom(ctx)
	meta, ok, err := s.Store.Attachment(ctx, consumer, r.PathValue("id"))
	if err != nil {
		s.fail(w, consumer, "attachment", err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	body, header, err := s.Upstream.Attachment(ctx, meta.ID)
	if err != nil {
		s.Log.Error("attachment fetch failed", "consumer", consumer, "seq", meta.Seq, "error", s.Upstream.Redact(err.Error()))
		writeError(w, http.StatusBadGateway, "upstream error")
		return
	}
	defer func() { _ = body.Close() }()
	contentType := meta.ContentType
	if contentType == "" {
		contentType = header.Get("Content-Type")
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if n := header.Get("Content-Length"); n != "" {
		w.Header().Set("Content-Length", n)
	}
	if meta.Filename != "" {
		if cd := mime.FormatMediaType("attachment", map[string]string{"filename": meta.Filename}); cd != "" {
			w.Header().Set("Content-Disposition", cd)
		}
	}
	if _, err := io.Copy(w, body); err != nil {
		s.Log.Warn("attachment stream aborted", "consumer", consumer, "seq", meta.Seq)
	}
}
