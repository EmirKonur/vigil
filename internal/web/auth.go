package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	sessionCookie = "vigil_session"
	sessionTTL    = 7 * 24 * time.Hour
)

// auth guards the dashboard with a single shared admin token. Sessions are
// stateless: the cookie holds an expiry and an HMAC of it, keyed by a hash
// of the token, so rotating VIGIL_ADMIN_TOKEN logs everyone out.
type auth struct {
	token  string
	key    []byte
	secure bool
}

func newAuth(token string, secure bool) *auth {
	key := sha256.Sum256([]byte("vigil-session\x00" + token))
	return &auth{token: token, key: key[:], secure: secure}
}

func (a *auth) sign(exp int64) string {
	mac := hmac.New(sha256.New, a.key)
	mac.Write([]byte(strconv.FormatInt(exp, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

func (a *auth) issue(w http.ResponseWriter, now time.Time) {
	exp := now.Add(sessionTTL).Unix()
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: strconv.FormatInt(exp, 10) + "." + a.sign(exp),
		Path: "/admin", HttpOnly: true, Secure: a.secure, SameSite: http.SameSiteLaxMode,
		Expires: time.Unix(exp, 0),
	})
}

func (a *auth) clear(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/admin", MaxAge: -1, HttpOnly: true, Secure: a.secure, SameSite: http.SameSiteLaxMode})
}

func (a *auth) tokenMatches(candidate string) bool {
	return a.token != "" && subtle.ConstantTimeCompare([]byte(candidate), []byte(a.token)) == 1
}

func (a *auth) valid(r *http.Request, now time.Time) bool {
	if bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return a.tokenMatches(bearer)
	}
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return false
	}
	expStr, sig, ok := strings.Cut(c.Value, ".")
	if !ok {
		return false
	}
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil || now.Unix() > exp {
		return false
	}
	return hmac.Equal([]byte(sig), []byte(a.sign(exp)))
}

func (h *Handler) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.auth.valid(r, h.opts.Clock.Now()) {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodGet && r.Header.Get("HX-Request") == "" && !strings.HasSuffix(r.URL.Path, "/events") {
			http.Redirect(w, r, "/admin/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
			return
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

// safeNext only allows redirects back into the dashboard.
func safeNext(next string) string {
	if strings.HasPrefix(next, "/admin") && !strings.HasPrefix(next, "//") && !strings.Contains(next, "\\") {
		return next
	}
	return "/admin"
}

func (h *Handler) loginPage(w http.ResponseWriter, r *http.Request) {
	if h.auth.valid(r, h.opts.Clock.Now()) {
		http.Redirect(w, r, safeNext(r.URL.Query().Get("next")), http.StatusSeeOther)
		return
	}
	h.views.render(w, r, http.StatusOK, "login", page{Title: "Sign in", Data: loginView{Next: r.URL.Query().Get("next")}})
}

type loginView struct {
	Next  string
	Error string
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	next := r.FormValue("next")
	if !h.auth.tokenMatches(r.FormValue("token")) {
		// A fixed delay makes guessing slow without any lockout state.
		time.Sleep(400 * time.Millisecond)
		h.log.Warn("failed dashboard login", "remote", r.RemoteAddr)
		h.views.render(w, r, http.StatusUnauthorized, "login", page{Title: "Sign in", Data: loginView{Next: next, Error: "That token is not right."}})
		return
	}
	h.auth.issue(w, h.opts.Clock.Now())
	http.Redirect(w, r, safeNext(next), http.StatusSeeOther)
}

func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	h.auth.clear(w)
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}
