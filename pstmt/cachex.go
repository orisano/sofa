package pstmt

import (
	"context"
	"database/sql"
	"sync"

	"github.com/jmoiron/sqlx"
)

type Cachex struct {
	*sqlx.DB
	stmts map[string]*sqlx.Stmt
	mu    sync.RWMutex
}

func NewCachex(db *sqlx.DB, err error) (*Cachex, error) {
	if err != nil {
		return nil, err
	}
	return &Cachex{DB: db}, nil
}

func (c *Cachex) stmt(ctx context.Context, query string) (*sqlx.Stmt, error) {
	c.mu.RLock()
	stmt, ok := c.stmts[query]
	c.mu.RUnlock()
	if ok {
		return stmt, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if stmt, ok := c.stmts[query]; ok {
		return stmt, nil
	}
	stmt, err := c.DB.PreparexContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if c.stmts == nil {
		c.stmts = map[string]*sqlx.Stmt{}
	}
	c.stmts[query] = stmt
	return stmt, nil
}

func (c *Cachex) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return sqlx.SelectContext(ctx, c, dest, query, args...)
}

func (c *Cachex) Select(dest interface{}, query string, args ...interface{}) error {
	return sqlx.Select(c, dest, query, args...)
}

func (c *Cachex) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return sqlx.GetContext(ctx, c, dest, query, args...)
}

func (c *Cachex) Get(dest interface{}, query string, args ...interface{}) error {
	return sqlx.Get(c, dest, query, args...)
}

func (c *Cachex) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryxContext(ctx, args...)
}

func (c *Cachex) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return c.QueryxContext(context.Background(), query, args...)
}

func (c *Cachex) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (c *Cachex) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.QueryContext(context.Background(), query, args...)
}

func (c *Cachex) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return c.DB.QueryRowxContext(ctx, query, args...)
	}
	return stmt.QueryRowxContext(ctx, args...)
}

func (c *Cachex) QueryxRow(query string, args ...interface{}) *sqlx.Row {
	return c.QueryRowxContext(context.Background(), query, args...)
}

func (c *Cachex) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return c.DB.QueryRowContext(ctx, query, args...)
	}
	return stmt.QueryRowContext(ctx, args...)
}

func (c *Cachex) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.QueryRowContext(context.Background(), query, args...)
}

func (c *Cachex) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (c *Cachex) Exec(query string, args ...interface{}) (sql.Result, error) {
	return c.ExecContext(context.Background(), query, args...)
}

func (c *Cachex) Close() error {
	c.mu.Lock()
	c.stmts = nil
	c.mu.Unlock()
	return c.DB.Close()
}

func (c *Cachex) Direct() *sqlx.DB {
	return c.DB
}
