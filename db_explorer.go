package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type any = interface{}

const (
	defaultLimit  = 5
	defaultOffset = 0
)

var (
	errUnknownTable   = &httpError{http.StatusNotFound, "unknown table"}
	errRecordNotFound = &httpError{http.StatusNotFound, "record not found"}
)

func errInvalidFieldType(field string) *httpError {
	return &httpError{http.StatusBadRequest, fmt.Sprintf("field %s have invalid type", field)}
}

func errFieldIsRequired(field string) *httpError {
	return &httpError{http.StatusBadRequest, fmt.Sprintf("field %s is required", field)}
}

type ID int

const ZeroID = ID(0)

type tEntry map[string]any

func newEntry(names []string, values []any) map[string]any {
	if len(names) != len(values) {
		panic("newEntry: len names and len values must be equal")
	}
	entry := make(map[string]any, len(values))
	for i := range names {
		entry[names[i]] = values[i]
	}
	return entry
}

type tTable struct {
	name   string
	fields []tField
	key    string
	fmap   map[string]*tField
}

func newTable(name string, fields []tField) *tTable {
	var key string
	fmap := make(map[string]*tField)
	for i := range fields {
		fmap[fields[i].name] = &fields[i]
		if fields[i].pimaryKey {
			key = fields[i].name
		}
	}

	return &tTable{
		name:   name,
		fields: fields,
		key:    key,
		fmap:   fmap,
	}
}

func (t *tTable) fieldNames() []string {
	list := make([]string, len(t.fields))
	for i := range t.fields {
		list[i] = t.fields[i].name
	}
	return list
}

func (t *tTable) newFieldValues() []any {
	list := make([]any, len(t.fields))
	for i := range t.fields {
		list[i] = t.fields[i].newValue()
	}
	return list
}

type tField struct {
	name          string
	kind          reflect.Kind
	pimaryKey     bool
	autoIncrement bool
	nullable      bool
	hasDefault    bool
}

func (f tField) newValue() any {
	switch f.kind {
	case reflect.Int:
		return new(sql.NullInt64)
	case reflect.String:
		return new(sql.NullString)
	case reflect.Float64:
		return new(sql.NullFloat64)
	default:
		panic(fmt.Errorf("unsupported field type %+v", f.kind)) // xxx
	}
}

func resolveValues(values []any) {
	for i, v := range values {
		switch v := v.(type) {
		case *sql.NullInt64:
			if !v.Valid {
				values[i] = nil
			} else {
				values[i] = v.Int64
			}
		case *sql.NullString:
			if !v.Valid {
				values[i] = nil
			} else {
				values[i] = v.String
			}
		case *sql.NullFloat64:
			if !v.Valid {
				values[i] = nil
			} else {
				values[i] = v.Float64
			}
		}
	}
}

func (f tField) parseValue(s string) (any, error) {
	if s == "null" {
		if !f.nullable {
			return nil, fmt.Errorf("%s cannot be null", f.name)
		}
		return nil, nil
	}

	switch f.kind {
	case reflect.Int:
		return strconv.Atoi(s)
	case reflect.String:
		return s, nil
	case reflect.Float64:
		return strconv.ParseFloat(s, 64)
	default:
		panic(fmt.Errorf("unsupported field type %+v", f.kind)) // xxx
	}
}

func (f tField) convertType(v any) (any, bool) {
	if v == nil {
		return nil, f.nullable
	}

	rv := reflect.ValueOf(v)

	if rv.Kind() == f.kind {
		return v, true
	}

	// json в пустой итнерфейс распаковывает как float
	if rv.Kind() == reflect.Float64 && f.kind == reflect.Int64 {
		v := rv.Float()
		if math.Round(v) == v {
			return int(v), true
		}
	}

	return nil, false
}

type DbExplorer struct {
	db     *sql.DB
	tables []tTable
	tmap   map[string]*tTable
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	h := &DbExplorer{
		db: db,
	}

	tableNames, err := h.listTables()
	if err != nil {
		return nil, err
	}

	h.tables = make([]tTable, 0, len(tableNames))
	for _, tableName := range tableNames {
		fields, err := h.listFields(tableName)
		if err != nil {
			return nil, err
		}
		h.tables = append(h.tables, *newTable(tableName, fields))
	}

	log.Printf("found tables: %+v", h.tables)

	h.tmap = make(map[string]*tTable, len(h.tables))
	for i := range h.tables {
		h.tmap[h.tables[i].name] = &h.tables[i]
	}

	return h, nil
}

func (h *DbExplorer) TableNames() []string {
	list := make([]string, len(h.tables))
	for i := range h.tables {
		list[i] = h.tables[i].name
	}
	return list
}

func (h *DbExplorer) listTables() ([]string, error) {

	rows, err := h.db.Query("show tables;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func (h *DbExplorer) listFields(tableName string) ([]tField, error) {

	rows, err := h.db.Query(fmt.Sprintf("show full columns from `%s`;", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// columns: Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment
	const (
		cField = iota
		cType
		_
		cNull
		cKey
		cDefault
		cExtra
	)

	columns := make([]sql.RawBytes, 9)
	p := make([]any, len(columns))
	for i := range columns {
		p[i] = &columns[i]
	}

	var fields []tField

	toLowerStr := func(b []byte) string {
		return strings.ToLower(unsafeString(b))
	}

	for rows.Next() {
		if err := rows.Scan(p...); err != nil {
			return nil, err
		}

		var f tField
		f.name = string(columns[cField])

		switch v := toLowerStr(columns[cType]); {
		case containsAny(v, "char", "text"):
			f.kind = reflect.String
		case containsAny(v, "int"):
			f.kind = reflect.Int
		case containsAny(v, "float", "real", "double"):
			f.kind = reflect.Float64
		}

		if v := toLowerStr(columns[cKey]); v == "pri" {
			f.pimaryKey = true
		}

		if v := toLowerStr(columns[cNull]); v == "yes" {
			f.nullable = true
		}

		if columns[cDefault] != nil {
			f.hasDefault = true
		}

		if v := toLowerStr(columns[cExtra]); containsAny(v, "auto_increment") {
			f.autoIncrement = true
		}

		fields = append(fields, f)
	}

	return fields, nil
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))

}

func containsAny(s string, a ...string) bool {
	for _, it := range a {
		if strings.Contains(s, it) {
			return true
		}
	}
	return false
}

var ErrTableNotExists = errors.New("table not exists")

func (h *DbExplorer) List(tableName string, limit, offset uint) ([]tEntry, error) {
	log.Printf("DbExplorer.List: table=%s, limit=%d, offset=%d", tableName, limit, offset)

	t, ok := h.tmap[tableName]
	if !ok {
		return nil, ErrTableNotExists
	}

	names := t.fieldNames()
	q := fmt.Sprintf("select `%s` from `%s` order by `%s` limit %d offset %d;", strings.Join(names, "`,`"), t.name, t.key, limit, offset)
	log.Printf("DbExplorer.List: q=%s", q)

	rows, err := h.db.Query(q)
	if err != nil {
		return nil, err
	}

	var list []tEntry

	for rows.Next() {
		values := t.newFieldValues()
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		resolveValues(values)
		list = append(list, newEntry(names, values))
	}

	return list, nil
}

func (h *DbExplorer) Get(tableName string, id ID) (tEntry, error) {
	log.Printf("DbExplorer.Get: table=%s, id=%d", tableName, id)

	t, ok := h.tmap[tableName]
	if !ok {
		return nil, ErrTableNotExists
	}

	names := t.fieldNames()
	values := t.newFieldValues()
	q := fmt.Sprintf("select `%s` from `%s` where `%s`=?;", strings.Join(names, "`,`"), t.name, t.key)
	log.Printf("DbExplorer.Get: q=%s", q)

	err := h.db.QueryRow(q, id).Scan(values...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	resolveValues(values)
	return newEntry(names, values), nil
}

func (h *DbExplorer) Delete(tableName string, id ID) (bool, error) {
	log.Printf("DbExplorer.Delete: table=%s, id=%d", tableName, id)

	t, ok := h.tmap[tableName]
	if !ok {
		return false, ErrTableNotExists
	}

	q := fmt.Sprintf("delete from `%s` where `%s`=?;", t.name, t.key)
	log.Printf("DbExplorer.Delete: q=%s", q)

	res, err := h.db.Exec(q, id)
	if err != nil {
		return false, err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return n != 0, nil
}

func valuesTempl(n int) string {
	templ := strings.Repeat(",?", n)
	if len(templ) > 0 {
		templ = templ[1:]
	}
	return templ
}

func (h *DbExplorer) Create(tableName string, names []string, values []any) (ID, error) {
	log.Printf("DbExplorer.Create: table=%s, names=%v, values=%v", tableName, names, values)

	t, ok := h.tmap[tableName]
	if !ok {
		return ZeroID, ErrTableNotExists
	}

	q := fmt.Sprintf("insert into `%s` (`%s`) values (%s);", t.name, strings.Join(names, "`,`"), valuesTempl(len(names)))
	log.Printf("DbExplorer.Create: q=%s", q)

	res, err := h.db.Exec(q, values...)
	if err != nil {
		return ZeroID, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return ZeroID, err
	}

	return ID(id), nil
}

func setFieldsTempl(names []string) string {
	sb := &strings.Builder{}
	for _, fn := range names {
		sb.WriteString(fmt.Sprintf(",`%s`=?", fn))
	}
	templ := sb.String()
	if len(templ) > 0 {
		templ = templ[1:]
	}
	return templ
}

func (h *DbExplorer) Update(tableName string, id ID, names []string, values []any) (bool, error) {
	log.Printf("DbExplorer.Update: table=%s, id=%d, names=%v, values=%v", tableName, id, names, values)

	t, ok := h.tmap[tableName]
	if !ok {
		return false, ErrTableNotExists
	}

	q := fmt.Sprintf("update `%s` set %s where `%s`=?;", t.name, setFieldsTempl(names), t.key)
	log.Printf("DbExplorer.Update: q=%s", q)

	res, err := h.db.Exec(q, append(values, id)...)
	if err != nil {
		return false, err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return n != 0, nil
}

func (h *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	p := strings.Split(r.URL.Path[1:], "/")
	if len(p) > 0 && p[len(p)-1] == "" {
		p = p[:len(p)-1]
	}

	if len(p) == 0 {
		resp := &struct {
			Tables []string `json:"tables"`
		}{
			h.TableNames(),
		}
		writeResponse(w, resp)
		return
	}

	if len(p) == 1 {
		table, ok := h.tmap[p[0]]
		if !ok {
			writeError(w, errUnknownTable)
			return
		}

		r.ParseForm()

		switch r.Method {
		case http.MethodGet:

			// если пришло не число на вход - берём дефолтное значене для лимита-оффсета
			var limit, offset uint = defaultLimit, defaultOffset
			if v, err := strconv.Atoi(r.Form.Get("limit")); err == nil && v >= 1 {
				limit = uint(v)
			}
			if v, err := strconv.Atoi(r.Form.Get("offset")); err == nil && v >= 0 {
				offset = uint(v)
			}

			list, err := h.List(table.name, limit, offset)
			if err != nil {
				writeError(w, err)
				return
			}

			resp := &struct {
				Records []tEntry `json:"records"`
			}{
				list,
			}

			writeResponse(w, resp)

		case http.MethodPut:

			names := make([]string, 0, len(table.fields))
			values := make([]any, 0, len(table.fields))

			req := map[string]any{}
			json.NewDecoder(r.Body).Decode(&req)

			for _, f := range table.fields {
				if f.pimaryKey && f.autoIncrement {
					// auto increment primary key игнорируется при вставке
					continue
				}
				if v, ok := req[f.name]; ok {
					v, ok := f.convertType(v)
					if !ok {
						writeError(w, errInvalidFieldType(f.name))
						return
					}
					names = append(names, f.name)
					values = append(values, v)
				} else if !f.nullable && !f.hasDefault {
					writeError(w, errFieldIsRequired(f.name))
					return
				}
			}

			id, err := h.Create(table.name, names, values)
			if err != nil {
				writeError(w, err)
				return
			}

			resp := newEntry([]string{table.key}, []any{id})
			writeResponse(w, resp)

		default:
			writeError(w, httpError{http.StatusMethodNotAllowed, "method not allowed"})
		}

		return
	}

	if len(p) == 2 {
		table, ok := h.tmap[p[0]]
		if !ok {
			writeError(w, errUnknownTable)
			return
		}

		entryID, err := strconv.Atoi(p[1])
		if err != nil || entryID < 1 {
			writeError(w, httpError{http.StatusBadRequest, "entry id must be int >= 1"})
			return
		}
		r.ParseForm()

		switch r.Method {
		case http.MethodGet:
			entry, err := h.Get(table.name, ID(entryID))
			if err != nil {
				writeError(w, err)
				return
			}
			if entry == nil {
				writeError(w, errRecordNotFound)
				return
			}
			resp := &struct {
				Record tEntry `json:"record"`
			}{
				entry,
			}
			writeResponse(w, resp)

		case http.MethodDelete:
			ok, err := h.Delete(table.name, ID(entryID))
			if err != nil {
				writeError(w, err)
				return
			}
			resp := &struct {
				Deleted int `json:"deleted"`
			}{}
			if ok {
				resp.Deleted = 1
			}
			writeResponse(w, resp)

		case http.MethodPost:
			names := make([]string, 0, len(table.fields))
			values := make([]any, 0, len(table.fields))

			req := map[string]any{}
			json.NewDecoder(r.Body).Decode(&req)

			for _, f := range table.fields {
				if v, ok := req[f.name]; ok {
					if f.pimaryKey {
						// primary key нельзя обновлять у существующей записи
						writeError(w, errInvalidFieldType(f.name))
						return
					}
					v, ok := f.convertType(v)
					if !ok {
						writeError(w, errInvalidFieldType(f.name))
						return
					}
					names = append(names, f.name)
					values = append(values, v)
				}
			}

			ok, err := h.Update(table.name, ID(entryID), names, values)
			if err != nil {
				writeError(w, err)
				return
			}

			resp := &struct {
				Updated int `json:"updated"`
			}{}
			if ok {
				resp.Updated = 1
			}
			writeResponse(w, resp)

		default:
			writeError(w, httpError{http.StatusMethodNotAllowed, "method not allowed"})
		}
		return
	}

	writeError(w, httpError{http.StatusNotFound, "method not found"})
}

func writeJSONResponse(w http.ResponseWriter, status int, resp any) {
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("can't write response: %v", err)
	}
}

type httpError struct {
	status int
	msg    string
}

func (e httpError) Error() string {
	return e.msg
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch err := err.(type) {
	case *httpError:
		status = err.status
	case httpError:
		status = err.status
	default:
		log.Printf("%d: %v", status, err)
	}
	resp := &struct {
		Error string `json:"error"`
	}{
		err.Error(),
	}
	writeJSONResponse(w, status, resp)
}

func writeResponse(w http.ResponseWriter, v any) {
	writeJSONResponse(w, http.StatusOK, &struct {
		Response any `json:"response"`
	}{v})
}
