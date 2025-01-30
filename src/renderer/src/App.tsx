import { useState, useEffect } from 'react';
import { Navbar } from '@/components/navbar';
import { BookGrid } from '@/components/book-grid';
import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, AlertCircle } from 'lucide-react';
import { Book } from '../../backend/models';

function App(): JSX.Element {
  const [books, setBooks] = useState<Book[]>([]);
  const [selectedBooks, setSelectedBooks] = useState<Set<string>>(new Set());
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isExporting, setIsExporting] = useState(false);

  useEffect(() => {
    loadBooks();
  }, []);

  const loadBooks = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const loadedBooks = await window.api.getBooks();
      setBooks(loadedBooks);
    } catch (error) {
      console.error('Error loading books:', error);
      setError('Failed to load the books, please check the file path at Settings.');
    } finally {
      setIsLoading(false);
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

  const renderContent = () => {
    if (isLoading) {
      return (
        <div className="flex flex-col items-center justify-center h-full space-y-4">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
          <p className="text-lg text-muted-foreground">Loading books...</p>
        </div>
      );
    }

    if (error) {
      return (
        <div className="flex flex-col items-center justify-center h-full space-y-6 p-4">
          <Alert variant="destructive" className="max-w-md flex space-x-2 p-2">
            <AlertCircle className="w-4" />
            <AlertDescription className="text-md">{error}</AlertDescription>
          </Alert>
          <Button onClick={loadBooks} variant="outline">
            <Loader2 className="mr-2 h-4 w-4" />
            Retry
          </Button>
        </div>
      );
    }

    return (
      <>
        <div className="flex justify-between items-center p-4 pb-0">
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
      </>
    );
  };

  return (
    <div className="h-screen flex flex-col w-full">
      <Navbar />
      <main className="flex-1 overflow-hidden">
        <div className="container mx-auto h-full">
          {renderContent()}
        </div>
      </main>
    </div>
  );
}

export default App;
