/* eslint-disable @typescript-eslint/ban-ts-comment, no-constant-binary-expression */
// @ts-nocheck
import { cn, formatAuthors } from './utils'

describe('cn', () => {
  it('should merge class names', () => {
    expect(cn('foo', 'bar')).toBe('foo bar')
  })

  it('should handle conditional classes', () => {
    expect(cn('base', false && 'hidden', 'visible')).toBe('base visible')
  })

  it('should merge tailwind classes, last wins', () => {
    expect(cn('p-4', 'p-2')).toBe('p-2')
  })

  it('should handle undefined and null inputs', () => {
    expect(cn('foo', undefined, null, 'bar')).toBe('foo bar')
  })

  it('should handle empty input', () => {
    expect(cn()).toBe('')
  })

  it('should merge conflicting tailwind utilities', () => {
    expect(cn('text-red-500', 'text-blue-500')).toBe('text-blue-500')
  })

  it('should handle array inputs', () => {
    expect(cn(['foo', 'bar'])).toBe('foo bar')
  })
})

describe('formatAuthors', () => {
  it('should return single author as-is', () => {
    expect(formatAuthors('John Doe')).toBe('John Doe')
  })

  it('should return two authors joined by comma', () => {
    expect(formatAuthors('John Doe, Jane Smith')).toBe('John Doe, Jane Smith')
  })

  it('should return three authors joined by comma', () => {
    expect(formatAuthors('A, B, C')).toBe('A, B, C')
  })

  it('should truncate after three authors and show remaining count', () => {
    expect(formatAuthors('A, B, C, D')).toBe('A, B, C, 1 more')
  })

  it('should show correct remaining count for many authors', () => {
    expect(formatAuthors('A, B, C, D, E, F')).toBe('A, B, C, 3 more')
  })

  it('should handle empty string', () => {
    expect(formatAuthors('')).toBe('')
  })

  it('should handle falsy-like input gracefully', () => {
    // The function uses (author || '') so it handles undefined-ish cases
    expect(formatAuthors('')).toBe('')
  })
})
