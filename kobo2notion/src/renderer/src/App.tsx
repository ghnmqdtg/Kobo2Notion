import { useState, useEffect } from 'react';
import { Navbar } from '@/components/navbar';
import { BookGrid } from '@/components/book-grid';
import { Button } from '@/components/ui/button';
import { Book } from '../../backend/models';

function App(): JSX.Element {
  const [books, setBooks] = useState<Book[]>([]);
  const [selectedBooks, setSelectedBooks] = useState<Set<string>>(new Set());
  const [isExporting, setIsExporting] = useState(false);

  useEffect(() => {
    // Load books when component mounts
    loadBooks();
  }, []);

  const loadBooks = async () => {
    try {
      const loadedBooks = await window.api.getBooks();
      setBooks(loadedBooks);
    } catch (error) {
      console.error('Error loading books:', error);
    }
  };

  const handleSelectBook = (bookTitle: string) => {
    setSelectedBooks((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(bookTitle)) {
        newSet.delete(bookTitle);
      } else {
        newSet.add(bookTitle);
      }
      return newSet;
    });
  };

  const handleExport = async () => {
    if (selectedBooks.size === 0) return;

    setIsExporting(true);
    try {
      for (const bookTitle of selectedBooks) {
        const book = books.find((b) => b.bookTitle === bookTitle);
        if (!book) continue;

        await window.api.exportBook(book);
      }
      // Clear selection after successful export
      setSelectedBooks(new Set());
    } catch (error) {
      console.error('Error exporting books:', error);
    } finally {
      setIsExporting(false);
    }
  };

  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <main className="flex-grow overflow-y-auto">
        <div className="container mx-auto py-4 h-full">
          <div className="flex justify-between items-center mb-4 px-4">
            <h1 className="text-2xl font-bold">Your Books</h1>
            <Button
              onClick={handleExport}
              disabled={selectedBooks.size === 0 || isExporting}
            >
              {isExporting ? 'Exporting...' : 'Export to Notion'}
            </Button>
          </div>
          <BookGrid
            books={books}
            selectedBooks={selectedBooks}
            onSelectBook={handleSelectBook}
          />
        </div>
      </main>
    </div>
  );
}

export default App;
