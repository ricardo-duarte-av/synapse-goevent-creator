package writer

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestQueriesUseOnlyGrants: every table a query names is in Grants, with the
// privilege the query needs (goinbound's test of the same name).
func TestQueriesUseOnlyGrants(t *testing.T) {
	granted := map[string]string{}
	for _, g := range Grants {
		granted[g.Table] = g.Privileges
	}
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	uses := []struct {
		re   *regexp.Regexp
		priv string
	}{
		{regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+(\w+)`), "INSERT"},
		{regexp.MustCompile(`(?i)\bUPDATE\s+(\w+)\s+SET`), "UPDATE"},
		{regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+(\w+)`), "DELETE"},
		{regexp.MustCompile(`(?i)\bFROM\s+(\w+)`), "SELECT"},
		{regexp.MustCompile(`(?i)\bJOIN\s+(\w+)`), "SELECT"},
	}
	notTables := map[string]bool{"unnest": true}
	queries := 0
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				vs, ok := n.(*ast.ValueSpec)
				if !ok {
					return true
				}
				for i, id := range vs.Names {
					if !strings.HasPrefix(id.Name, "q") || i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					sql, _ := strconv.Unquote(lit.Value)
					queries++
					for _, u := range uses {
						for _, m := range u.re.FindAllStringSubmatch(sql, -1) {
							if notTables[m[1]] {
								continue
							}
							privs, ok := granted[m[1]]
							if !ok {
								t.Errorf("%s uses %s, which is not in Grants", id.Name, m[1])
								continue
							}
							if !strings.Contains(privs, u.priv) {
								t.Errorf("%s needs %s on %s, granted %q", id.Name, u.priv, m[1], privs)
							}
						}
					}
				}
				return true
			})
		}
	}
	if queries < 5 {
		t.Fatalf("found %d queries; the test is not looking at the code", queries)
	}
}

// TestGrantsAreNarrow: the role may write only what Synapse's event creator
// writes. A new write grant must be added here on purpose.
func TestGrantsAreNarrow(t *testing.T) {
	writes := map[string]string{
		"event_push_actions_staging":    "SELECT, INSERT, DELETE",
		"room_memberships":              "SELECT, UPDATE (participant)",
		"local_current_membership":      "SELECT",
		"events":                        "SELECT",
		"state_groups":                  "SELECT",
		"state_groups_pending_deletion": "SELECT, UPDATE",
	}
	if len(Grants) != len(writes) {
		t.Fatalf("Grants has %d tables, the test knows %d", len(Grants), len(writes))
	}
	for _, g := range Grants {
		if writes[g.Table] != g.Privileges {
			t.Errorf("%s: granted %q, expected %q", g.Table, g.Privileges, writes[g.Table])
		}
	}
}
