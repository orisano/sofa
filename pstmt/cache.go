package pstmt

import (
	"context"
	"database/sql"
	"sync"
)

type Cache struct {
	*sql.DB
	stmts map[string]*sql.Stmt
	mu    sync.RWMutex
}

func NewCache(db *sql.DB, err error) (*Cache, error) {
	if err != nil {
		return nil, err
	}
	return &Cache{DB: db}, nil
}

func (c *Cache) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
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
	stmt, err := c.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if c.stmts == nil {
		c.stmts = map[string]*sql.Stmt{}
	}
	c.stmts[query] = stmt
	return stmt, nil
}

func (c *Cache) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (c *Cache) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.QueryContext(context.Background(), query, args...)
}

func (c *Cache) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return c.DB.QueryRowContext(ctx, query, args...)
	}
	return stmt.QueryRowContext(ctx, args...)
}

func (c *Cache) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.QueryRowContext(context.Background(), query, args...)
}

func (c *Cache) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := c.stmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (c *Cache) Exec(query string, args ...interface{}) (sql.Result, error) {
	return c.ExecContext(context.Background(), query, args...)
}

func (c *Cache) Close() error {
	c.mu.Lock()
	c.stmts = nil
	c.mu.Unlock()
	return c.DB.Close()
}

func (c *Cache) Direct() *sql.DB {
	return c.DB
}
