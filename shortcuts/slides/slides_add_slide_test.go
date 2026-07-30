// Copyright (c) 2026 Lark Technologies Pte. Ltd.
// SPDX-License-Identifier: MIT

package slides

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/larksuite/cli/errs"
	"github.com/larksuite/cli/internal/cmdutil"
	"github.com/larksuite/cli/internal/httpmock"
)

const testSlideXML = `<slide xmlns="http://www.larkoffice.com/sml/2.0"><data><shape type="text" topLeftX="80" topLeftY="80" width="800" height="120"><content textType="title"><p>hi</p></content></shape></data></slide>`

func TestAddSlideDeclaredScopes(t *testing.T) {
	// Pre-flight must stay at the two slides scopes: an XML-only page needs
	// neither wiki read nor media upload, and gating every call on them would
	// force unrelated consent.
	want := []string{"slides:presentation:update", "slides:presentation:write_only"}
	for _, identity := range []string{"user", "bot"} {
		if got := SlidesAddSlide.ScopesForIdentity(identity); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s preflight scopes = %#v, want %#v", identity, got, want)
		}
	}
	got := SlidesAddSlide.DeclaredScopesForIdentity("user")
	wantDeclared := []string{"slides:presentation:update", "slides:presentation:write_only", "wiki:node:read", "docs:document.media:upload"}
	if !reflect.DeepEqual(got, wantDeclared) {
		t.Fatalf("declared scopes = %#v, want %#v", got, wantDeclared)
	}
}

// TestAddSlideOmitsBeforeSlideIDWhenUnset guards the append case: the backend
// appends only when before_slide_id is absent, so sending "" would turn a
// plain append into a lookup of a slide that does not exist.
func TestAddSlideOmitsBeforeSlideIDWhenUnset(t *testing.T) {
	t.Parallel()

	f, stdout, _, reg := cmdutil.TestFactory(t, slidesTestConfig(t, ""))
	var gotQuery url.Values
	stub := &httpmock.Stub{
		Method:  "POST",
		URL:     "/open-apis/slides_ai/v1/xml_presentations/pres_abc/slide",
		Body:    map[string]interface{}{"code": 0, "data": map[string]interface{}{"slide_id": "slide_new", "revision_id": 9}},
		OnMatch: func(req *http.Request) { gotQuery = req.URL.Query() },
	}
	reg.Register(stub)

	err := runSlidesShortcut(t, f, stdout, SlidesAddSlide, []string{
		"+add-slide",
		"--presentation", "pres_abc",
		"--slide", testSlideXML,
		"--as", "user",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(stub.CapturedBody, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, ok := body["before_slide_id"]; ok {
		t.Fatalf("before_slide_id must be absent when --before-slide-id is unset, got body %s", stub.CapturedBody)
	}
	slide, _ := body["slide"].(map[string]interface{})
	if slide["content"] != testSlideXML {
		t.Fatalf("slide.content = %v, want the XML passed to --slide", slide["content"])
	}
	if gotQuery.Get("revision_id") != "-1" {
		t.Fatalf("revision_id query = %q, want -1", gotQuery.Get("revision_id"))
	}
	if _, ok := gotQuery["tid"]; ok {
		t.Fatalf("tid must be absent when --tid is unset, got %v", gotQuery)
	}

	data := decodeShortcutData(t, stdout)
	if data["slide_id"] != "slide_new" {
		t.Fatalf("slide_id = %v, want slide_new", data["slide_id"])
	}
	if data["revision_id"] != float64(9) {
		t.Fatalf("revision_id = %v, want 9", data["revision_id"])
	}
	if data["xml_presentation_id"] != "pres_abc" {
		t.Fatalf("xml_presentation_id = %v, want pres_abc", data["xml_presentation_id"])
	}
	if _, ok := data["before_slide_id"]; ok {
		t.Fatalf("output should not report before_slide_id when unset: %#v", data)
	}
}

func TestAddSlideSendsBeforeSlideIDAndTid(t *testing.T) {
	t.Parallel()

	f, stdout, _, reg := cmdutil.TestFactory(t, slidesTestConfig(t, ""))
	var gotQuery url.Values
	stub := &httpmock.Stub{
		Method:  "POST",
		URL:     "/open-apis/slides_ai/v1/xml_presentations/pres_abc/slide",
		Body:    map[string]interface{}{"code": 0, "data": map[string]interface{}{"slide_id": "slide_new", "revision_id": 3}},
		OnMatch: func(req *http.Request) { gotQuery = req.URL.Query() },
	}
	reg.Register(stub)

	err := runSlidesShortcut(t, f, stdout, SlidesAddSlide, []string{
		"+add-slide",
		"--presentation", "pres_abc",
		"--slide", testSlideXML,
		"--before-slide-id", "slide_target",
		"--revision-id", "12",
		"--tid", "tx_1",
		"--as", "user",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(stub.CapturedBody, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["before_slide_id"] != "slide_target" {
		t.Fatalf("before_slide_id = %v, want slide_target", body["before_slide_id"])
	}
	if gotQuery.Get("revision_id") != "12" {
		t.Fatalf("revision_id query = %q, want 12", gotQuery.Get("revision_id"))
	}
	if gotQuery.Get("tid") != "tx_1" {
		t.Fatalf("tid query = %q, want tx_1", gotQuery.Get("tid"))
	}

	data := decodeShortcutData(t, stdout)
	if data["before_slide_id"] != "slide_target" {
		t.Fatalf("output before_slide_id = %v, want slide_target", data["before_slide_id"])
	}
}

// TestAddSlideUploadsImagePlaceholder is the reason this shortcut exists for
// image-bearing pages: before it, adding one to an existing deck meant calling
// +media-upload and splicing the token in by hand.
func TestAddSlideUploadsImagePlaceholder(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "chart.png"), []byte("png-bytes"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	withSlidesTestWorkingDir(t, dir)

	f, stdout, _, reg := cmdutil.TestFactory(t, slidesTestConfig(t, ""))
	uploadStub := &httpmock.Stub{
		Method: "POST",
		URL:    "/open-apis/drive/v1/medias/upload_all",
		Body:   map[string]interface{}{"code": 0, "data": map[string]interface{}{"file_token": "tok_chart"}},
	}
	slideStub := &httpmock.Stub{
		Method: "POST",
		URL:    "/open-apis/slides_ai/v1/xml_presentations/pres_img/slide",
		Body:   map[string]interface{}{"code": 0, "data": map[string]interface{}{"slide_id": "s_img", "revision_id": 4}},
	}
	reg.Register(uploadStub)
	reg.Register(slideStub)

	// The same image referenced twice must still upload once.
	slideXML := `<slide xmlns="http://www.larkoffice.com/sml/2.0"><data>` +
		`<img src="@./chart.png" topLeftX="10" topLeftY="10" width="100" height="100"/>` +
		`<img src="@./chart.png" topLeftX="200" topLeftY="10" width="100" height="100"/>` +
		`</data></slide>`

	err := runSlidesShortcut(t, f, stdout, SlidesAddSlide, []string{
		"+add-slide",
		"--presentation", "pres_img",
		"--slide", slideXML,
		"--as", "user",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(slideStub.CapturedBody, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	slide, _ := body["slide"].(map[string]interface{})
	content, _ := slide["content"].(string)
	if strings.Contains(content, "@./chart.png") {
		t.Fatalf("placeholder was not rewritten: %s", content)
	}
	if strings.Count(content, `src="tok_chart"`) != 2 {
		t.Fatalf("both references should carry the uploaded token, got: %s", content)
	}

	data := decodeShortcutData(t, stdout)
	if data["images_uploaded"] != float64(1) {
		t.Fatalf("images_uploaded = %v, want 1 (deduped by path)", data["images_uploaded"])
	}
}

func TestAddSlideRejectsNonSlideRoot(t *testing.T) {
	t.Parallel()

	f, stdout, _, _ := cmdutil.TestFactory(t, slidesTestConfig(t, ""))
	err := runSlidesShortcut(t, f, stdout, SlidesAddSlide, []string{
		"+add-slide",
		"--presentation", "pres_abc",
		"--slide", `<presentation xmlns="http://www.larkoffice.com/sml/2.0"><slide/></presentation>`,
		"--as", "user",
	})
	if err == nil {
		t.Fatal("expected a validation error for a <presentation> root")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *errs.ValidationError", err)
	}
	// The flag tag is what lets an agent know which input to fix; the shared
	// XML validator alone reports only the structural problem.
	if ve.Param != "--slide" {
		t.Fatalf("Param = %q, want --slide", ve.Param)
	}
	if !strings.Contains(err.Error(), "want <slide>") {
		t.Fatalf("message should name the expected root element: %v", err)
	}
}

func TestAddSlideRejectsEmptySlide(t *testing.T) {
	t.Parallel()

	f, stdout, _, _ := cmdutil.TestFactory(t, slidesTestConfig(t, ""))
	err := runSlidesShortcut(t, f, stdout, SlidesAddSlide, []string{
		"+add-slide",
		"--presentation", "pres_abc",
		"--slide", "   ",
		"--as", "user",
	})
	if err == nil {
		t.Fatal("expected a validation error for a blank --slide")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) || ve.Param != "--slide" {
		t.Fatalf("want *errs.ValidationError on --slide, got %v", err)
	}
}

// TestAddSlideMissingImageFailsBeforeAnyCall proves the placeholder check runs
// in Validate: a bad path must not reach the API, or the caller is left
// guessing whether a page was created.
func TestAddSlideMissingImageFailsBeforeAnyCall(t *testing.T) {
	withSlidesTestWorkingDir(t, t.TempDir())

	// Register nothing on purpose: httpmock rejects any unstubbed request, so
	// reaching the API would surface as "no stub for POST .../slide" rather
	// than the ValidationError asserted below. That is the no-call assertion.
	f, stdout, _, _ := cmdutil.TestFactory(t, slidesTestConfig(t, ""))

	err := runSlidesShortcut(t, f, stdout, SlidesAddSlide, []string{
		"+add-slide",
		"--presentation", "pres_abc",
		"--slide", `<slide xmlns="x"><data><img src="@./missing.png"/></data></slide>`,
		"--as", "user",
	})
	if err == nil {
		t.Fatal("expected a validation error for a missing image file")
	}
	var ve *errs.ValidationError
	if !errors.As(err, &ve) || ve.Param != "--slide" {
		t.Fatalf("want *errs.ValidationError on --slide, got %v", err)
	}
	// The path came from an <img> inside the XML, not from an @file passed to
	// --slide. Naming the element keeps the caller from re-checking the flag
	// argument, which is what "--slide @./missing.png" used to imply.
	if !strings.Contains(err.Error(), `<img src="@./missing.png">`) {
		t.Fatalf("error should quote the <img> placeholder, got %v", err)
	}
}

func TestAddSlideResolvesWikiURL(t *testing.T) {
	t.Parallel()

	f, stdout, _, reg := cmdutil.TestFactory(t, slidesTestConfig(t, ""))
	reg.Register(&httpmock.Stub{
		Method: "GET",
		URL:    "/open-apis/wiki/v2/spaces/get_node",
		Body: map[string]interface{}{
			"code": 0,
			"data": map[string]interface{}{
				"node": map[string]interface{}{"obj_type": "slides", "obj_token": "pres_from_wiki"},
			},
		},
	})
	slideStub := &httpmock.Stub{
		Method: "POST",
		URL:    "/open-apis/slides_ai/v1/xml_presentations/pres_from_wiki/slide",
		Body:   map[string]interface{}{"code": 0, "data": map[string]interface{}{"slide_id": "s_wiki", "revision_id": 2}},
	}
	reg.Register(slideStub)

	err := runSlidesShortcut(t, f, stdout, SlidesAddSlide, []string{
		"+add-slide",
		"--presentation", "https://example.feishu.cn/wiki/wikcnTOKEN",
		"--slide", testSlideXML,
		"--as", "user",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	data := decodeShortcutData(t, stdout)
	if data["xml_presentation_id"] != "pres_from_wiki" {
		t.Fatalf("xml_presentation_id = %v, want the wiki-resolved token", data["xml_presentation_id"])
	}
}
