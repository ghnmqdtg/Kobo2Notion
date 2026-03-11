import { BookGrid } from '@/components/book-grid'
import { BookList } from '@/components/book-list'
import { Book } from '../../../../backend/models'

interface BookDisplayProps {
  isGridView: boolean
  books: Book[]
  selectedBooks: Set<string>
  onSelectBook: (bookTitle: string) => void
  onPreviewBookmarks: (bookTitle: string) => void
  isProcessing: boolean
  currentBook: string
  exportedBooks: Set<string>
}

export function BookDisplay({
  isGridView,
  books,
  selectedBooks,
  onSelectBook,
  onPreviewBookmarks,
  isProcessing,
  currentBook,
  exportedBooks
}: BookDisplayProps) {
  return (
    <div className="relative">
      {isGridView ? (
        <BookGrid
          books={books}
          selectedBooks={selectedBooks}
          onSelectBook={onSelectBook}
          onPreviewBookmarks={onPreviewBookmarks}
          isProcessing={isProcessing}
          currentBook={currentBook}
          exportedBooks={exportedBooks}
        />
      ) : (
        <BookList
          books={books}
          selectedBooks={selectedBooks}
          onSelectBook={onSelectBook}
          onPreviewBookmarks={onPreviewBookmarks}
          isProcessing={isProcessing}
          currentBook={currentBook}
          exportedBooks={exportedBooks}
        />
      )}
      <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-t from-background to-transparent pointer-events-none" />
    </div>
  )
}
