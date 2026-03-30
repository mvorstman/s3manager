package main

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"time"
)

// ── Template data types ──────────────────────────────────────────────────────

type uploadPageData struct {
	Error       string
	ExpiryHours int
}

type downloadPageData struct {
	*transferManifest
	TotalSize string
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func humanSize(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b < kb:
		return fmt.Sprintf("%d B", b)
	case b < mb:
		return fmt.Sprintf("%.1f KB", float64(b)/kb)
	case b < gb:
		return fmt.Sprintf("%.1f MB", float64(b)/mb)
	default:
		return fmt.Sprintf("%.2f GB", float64(b)/gb)
	}
}

func fmtTime(t time.Time) string {
	return t.UTC().Format("Jan 2, 2006 at 15:04 UTC")
}

// ── Templates ────────────────────────────────────────────────────────────────

var uploadTmpl = template.Must(template.New("upload").Parse(uploadHTML))

var downloadTmpl = template.Must(
	template.New("download").Funcs(template.FuncMap{
		"humanSize": humanSize,
		"fmtTime":   fmtTime,
		"urlEncode": url.PathEscape,
		"sub":       func(a, b int) int { return a - b },
	}).Parse(downloadHTML),
)

var notFoundTmpl = template.Must(template.New("notfound").Parse(notFoundHTML))
var expiredTmpl = template.Must(template.New("expired").Parse(expiredHTML))

// ── Render functions ─────────────────────────────────────────────────────────

func renderUploadPage(w http.ResponseWriter, data uploadPageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uploadTmpl.Execute(w, data); err != nil {
		log.Printf("upload template error: %v", err)
	}
}

func renderDownloadPage(w http.ResponseWriter, m *transferManifest) {
	var total int64
	for _, f := range m.Files {
		total += f.Size
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := downloadTmpl.Execute(w, downloadPageData{
		transferManifest: m,
		TotalSize:        humanSize(total),
	}); err != nil {
		log.Printf("download template error: %v", err)
	}
}

func renderNotFoundPage(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	if err := notFoundTmpl.Execute(w, nil); err != nil {
		log.Printf("notfound template error: %v", err)
	}
}

func renderExpiredPage(w http.ResponseWriter, m *transferManifest) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusGone)
	if err := expiredTmpl.Execute(w, m); err != nil {
		log.Printf("expired template error: %v", err)
	}
}

// ── HTML ─────────────────────────────────────────────────────────────────────

const baseCSS = `
	*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
	body {
		font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
		background: #f0efff;
		color: #111827;
		min-height: 100vh;
	}
	nav {
		background: #fff;
		border-bottom: 1px solid #e5e7eb;
		padding: 0 24px;
		height: 56px;
		display: flex;
		align-items: center;
	}
	.logo { font-weight: 700; font-size: 1.05rem; color: #6c63ff; letter-spacing: -0.3px; }
	.logo span { color: #374151; }
	main { display: flex; justify-content: center; padding: 48px 16px 80px; }
	.card {
		background: #fff;
		border-radius: 16px;
		box-shadow: 0 4px 24px rgba(108,99,255,0.10), 0 1px 4px rgba(0,0,0,0.06);
		padding: 40px;
		width: 100%;
		max-width: 560px;
	}
	h1 { font-size: 1.5rem; font-weight: 700; margin-bottom: 4px; }
	.subtitle { color: #6b7280; font-size: 0.9rem; margin-bottom: 28px; }
	a { color: #6c63ff; }
`

const uploadHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Transfer Portal</title>
<style>
` + baseCSS + `
	.error-banner {
		background: #fef2f2; color: #dc2626;
		border: 1px solid #fecaca; border-radius: 8px;
		padding: 12px 16px; margin-bottom: 20px; font-size: 0.875rem;
	}
	.drop-zone {
		border: 2px dashed #d1d5db;
		border-radius: 12px;
		padding: 40px 24px;
		text-align: center;
		cursor: pointer;
		transition: all 0.2s;
		background: #fafafa;
		user-select: none;
	}
	.drop-zone:hover, .drop-zone.active {
		border-color: #6c63ff;
		background: #f5f4ff;
	}
	.drop-zone svg {
		width: 44px; height: 44px;
		color: #9ca3af; margin-bottom: 12px;
		transition: color 0.2s;
	}
	.drop-zone.active svg, .drop-zone:hover svg { color: #6c63ff; }
	.drop-zone p { color: #6b7280; font-size: 0.95rem; }
	.drop-zone .browse { color: #6c63ff; font-weight: 600; text-decoration: underline; }
	.file-list { list-style: none; margin: 12px 0 0; display: flex; flex-direction: column; gap: 8px; }
	.file-item {
		display: flex; align-items: center; gap: 10px;
		background: #f9fafb; border: 1px solid #e5e7eb;
		border-radius: 8px; padding: 10px 14px; font-size: 0.875rem;
	}
	.file-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; }
	.file-sz { color: #6b7280; white-space: nowrap; font-size: 0.8rem; }
	.rm-btn {
		background: none; border: none; color: #9ca3af;
		cursor: pointer; font-size: 1rem; padding: 0 2px; line-height: 1;
		transition: color 0.15s; flex-shrink: 0;
	}
	.rm-btn:hover { color: #ef4444; }
	.form-group { margin-top: 20px; }
	.form-group textarea {
		width: 100%; border: 1px solid #d1d5db; border-radius: 8px;
		padding: 12px 14px; font-family: inherit; font-size: 0.9rem;
		resize: vertical; color: #111827; outline: none;
		transition: border-color 0.2s, box-shadow 0.2s;
	}
	.form-group textarea:focus {
		border-color: #6c63ff;
		box-shadow: 0 0 0 3px rgba(108,99,255,0.15);
	}
	.form-group textarea::placeholder { color: #9ca3af; }
	.btn-primary {
		display: block; width: 100%; margin-top: 24px;
		background: #6c63ff; color: #fff; border: none;
		border-radius: 10px; padding: 14px;
		font-size: 1rem; font-weight: 600; cursor: pointer;
		transition: background 0.2s, opacity 0.2s;
	}
	.btn-primary:hover:not(:disabled) { background: #5a52d5; }
	.btn-primary:disabled { opacity: 0.45; cursor: not-allowed; }
</style>
</head>
<body>
<nav><span class="logo">Transfer<span> Portal</span></span></nav>
<main>
  <div class="card">
    <h1>Send files</h1>
    <p class="subtitle">Recipients get a private link. Files expire after {{.ExpiryHours}} hours.</p>
    {{if .Error}}<div class="error-banner">{{.Error}}</div>{{end}}
    <form id="upload-form" action="/upload" method="POST" enctype="multipart/form-data">
      <div id="drop-zone" class="drop-zone">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M12 16.5V9.75m0 0l3 3m-3-3l-3 3M6.75 19.5a4.5 4.5 0 01-1.41-8.775 5.25 5.25 0 0110.338-2.32 5.25 5.25 0 011.413 6.926A3 3 0 0118 19.5H6.75z"/>
        </svg>
        <p>Drop files here or <span class="browse">browse</span></p>
        <input type="file" id="file-input" name="files" multiple hidden>
      </div>
      <ul id="file-list" class="file-list"></ul>
      <div class="form-group">
        <textarea name="message" placeholder="Add a message for the recipient (optional)" rows="3"></textarea>
      </div>
      <button type="submit" id="submit-btn" class="btn-primary" disabled>Create Transfer</button>
    </form>
  </div>
</main>
<script>
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');
const fileListEl = document.getElementById('file-list');
const submitBtn = document.getElementById('submit-btn');
const form = document.getElementById('upload-form');
let transfer = new DataTransfer();

dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('active'); });
dropZone.addEventListener('dragleave', e => { if (!dropZone.contains(e.relatedTarget)) dropZone.classList.remove('active'); });
dropZone.addEventListener('drop', e => { e.preventDefault(); dropZone.classList.remove('active'); addFiles(e.dataTransfer.files); });
fileInput.addEventListener('change', () => { addFiles(fileInput.files); fileInput.value = ''; });

function addFiles(files) {
  for (const f of files) transfer.items.add(f);
  render();
}

function removeFile(i) {
  const dt = new DataTransfer();
  [...transfer.files].forEach((f, j) => { if (j !== i) dt.items.add(f); });
  transfer = dt;
  render();
}

function render() {
  fileListEl.innerHTML = '';
  [...transfer.files].forEach((f, i) => {
    const li = document.createElement('li');
    li.className = 'file-item';
    li.innerHTML =
      '<span class="file-name" title="' + esc(f.name) + '">' + esc(f.name) + '</span>' +
      '<span class="file-sz">' + fmt(f.size) + '</span>' +
      '<button type="button" class="rm-btn" onclick="removeFile(' + i + ')" title="Remove">&#x2715;</button>';
    fileListEl.appendChild(li);
  });
  submitBtn.disabled = transfer.files.length === 0;
  fileInput.files = transfer.files;
}

function esc(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function fmt(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b/1048576).toFixed(1) + ' MB';
  return (b/1073741824).toFixed(2) + ' GB';
}

form.addEventListener('submit', () => {
  submitBtn.disabled = true;
  submitBtn.textContent = 'Sending\u2026';
});
</script>
</body>
</html>`

const downloadHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Transfer Portal – Download</title>
<style>
` + baseCSS + `
	.transfer-header { margin-bottom: 6px; }
	.badge {
		display: inline-flex; align-items: center; gap: 6px;
		background: #f0fdf4; color: #059669;
		border: 1px solid #bbf7d0;
		border-radius: 999px; padding: 4px 12px;
		font-size: 0.8rem; font-weight: 600;
		margin-bottom: 16px;
	}
	.badge svg { width: 14px; height: 14px; }
	.meta { color: #6b7280; font-size: 0.85rem; margin-bottom: 24px; }
	.message-box {
		background: #f9fafb; border-left: 3px solid #6c63ff;
		border-radius: 0 8px 8px 0;
		padding: 12px 16px; margin-bottom: 24px;
		font-size: 0.9rem; color: #374151; white-space: pre-wrap;
	}
	.file-row {
		display: flex; align-items: center; gap: 12px;
		padding: 12px 0;
		border-bottom: 1px solid #f3f4f6;
	}
	.file-row:last-child { border-bottom: none; }
	.file-icon { color: #9ca3af; flex-shrink: 0; }
	.file-icon svg { width: 20px; height: 20px; }
	.file-info { flex: 1; min-width: 0; }
	.file-info .fname {
		font-weight: 500; font-size: 0.9rem;
		overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
	}
	.file-info .fsize { color: #9ca3af; font-size: 0.8rem; }
	.btn-dl {
		background: #6c63ff; color: #fff;
		border: none; border-radius: 8px;
		padding: 8px 16px; font-size: 0.85rem; font-weight: 600;
		text-decoration: none; white-space: nowrap;
		transition: background 0.2s;
		flex-shrink: 0;
	}
	.btn-dl:hover { background: #5a52d5; color: #fff; }
	.transfer-footer {
		display: flex; justify-content: space-between; align-items: center;
		margin-top: 24px; padding-top: 20px;
		border-top: 1px solid #f3f4f6;
		font-size: 0.8rem; color: #9ca3af;
		gap: 12px; flex-wrap: wrap;
	}
	.copy-btn {
		background: none; border: 1px solid #e5e7eb; border-radius: 7px;
		padding: 6px 12px; font-size: 0.8rem; color: #6b7280;
		cursor: pointer; transition: all 0.2s; white-space: nowrap;
	}
	.copy-btn:hover { border-color: #6c63ff; color: #6c63ff; }
</style>
</head>
<body>
<nav><span class="logo">Transfer<span> Portal</span></span></nav>
<main>
  <div class="card">
    <div class="transfer-header">
      <span class="badge">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
        </svg>
        Ready to download
      </span>
      <h1>{{len .Files}} file{{if gt (len .Files) 1}}s{{end}}</h1>
    </div>
    <p class="meta">Expires {{fmtTime .ExpiresAt}} &nbsp;·&nbsp; Shared {{fmtTime .CreatedAt}}</p>
    {{if .Message}}<div class="message-box">{{.Message}}</div>{{end}}
    <div class="file-rows">
      {{range .Files}}
      <div class="file-row">
        <span class="file-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
            <path d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z"/>
          </svg>
        </span>
        <div class="file-info">
          <div class="fname" title="{{.Name}}">{{.Name}}</div>
          <div class="fsize">{{humanSize .Size}}</div>
        </div>
        <a href="/d/{{$.ID}}/{{.Name | urlEncode}}" class="btn-dl" download="{{.Name}}">Download</a>
      </div>
      {{end}}
    </div>
    <div class="transfer-footer">
      <span>Total: {{.TotalSize}}</span>
      <button class="copy-btn" id="copy-btn">Copy link</button>
    </div>
  </div>
</main>
<script>
document.getElementById('copy-btn').addEventListener('click', function() {
  navigator.clipboard.writeText(window.location.href).then(() => {
    this.textContent = 'Copied!';
    setTimeout(() => this.textContent = 'Copy link', 2000);
  }).catch(() => {
    this.textContent = 'Copy failed';
    setTimeout(() => this.textContent = 'Copy link', 2000);
  });
});
</script>
</body>
</html>`

const notFoundHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Transfer not found</title>
<style>` + baseCSS + `
  .card { text-align: center; padding: 60px 40px; }
  .icon { font-size: 3rem; margin-bottom: 16px; }
  p { color: #6b7280; margin-top: 8px; }
  .back { display: inline-block; margin-top: 24px; color: #6c63ff; font-weight: 600; text-decoration: none; }
</style>
</head>
<body>
<nav><span class="logo">Transfer<span> Portal</span></span></nav>
<main>
  <div class="card">
    <div class="icon">&#x1F50D;</div>
    <h1>Transfer not found</h1>
    <p>This link may be invalid or the transfer may have been deleted.</p>
    <a href="/" class="back">Send a new transfer</a>
  </div>
</main>
</body>
</html>`

const expiredHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Transfer expired</title>
<style>` + baseCSS + `
  .card { text-align: center; padding: 60px 40px; }
  .icon { font-size: 3rem; margin-bottom: 16px; }
  p { color: #6b7280; margin-top: 8px; }
  .exp { color: #ef4444; font-weight: 500; }
  .back { display: inline-block; margin-top: 24px; color: #6c63ff; font-weight: 600; text-decoration: none; }
</style>
</head>
<body>
<nav><span class="logo">Transfer<span> Portal</span></span></nav>
<main>
  <div class="card">
    <div class="icon">&#x23F3;</div>
    <h1>Transfer expired</h1>
    <p>This transfer expired on <span class="exp">{{fmtTime .ExpiresAt}}</span>.</p>
    <a href="/" class="back">Send a new transfer</a>
  </div>
</main>
</body>
</html>`
