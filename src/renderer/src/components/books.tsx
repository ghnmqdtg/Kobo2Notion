import { useState, useEffect } from 'react';
import { BookGrid } from '@/components/book-grid';
import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, AlertCircle } from 'lucide-react';
import { Footer } from '@/components/footer';
import { Book } from '../../../backend/models';

export function Books() {
    const [books, setBooks] = useState<Book[]>([]);
    const [selectedBooks, setSelectedBooks] = useState<Set<string>>(new Set());
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [isExporting, setIsExporting] = useState(false);
    const [retryCount, setRetryCount] = useState(0);
    const [exportProgress, setExportProgress] = useState({
        currentBook: '',
        currentStep: '',
        completed: 0
    });

    const maxRetries = 3;
    const retryInterval = 5000;

    useEffect(() => {
        loadBooks();
    }, []);

    useEffect(() => {
        let intervalId: NodeJS.Timeout;

        if (error && retryCount < maxRetries) {
            intervalId = setInterval(() => {
                setRetryCount((prevCount) => prevCount + 1);
                loadBooks();
            }, retryInterval);
        }

        return () => clearInterval(intervalId);
    }, [error, retryCount]);

    const loadBooks = async () => {
        setIsLoading(true);
        setError(null);
        try {
            const loadedBooks = await window.api.getBooks();
            setBooks(loadedBooks);
            setError(null);
            setRetryCount(0);
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
        let completed = 0;

        try {
            for (const bookTitle of selectedBooks) {
                const book = books.find((b) => b.bookTitle === bookTitle);
                if (!book) continue;

                setExportProgress({
                    currentBook: book.bookTitle,
                    currentStep: 'Exporting highlights...',
                    completed
                });

                await window.api.exportBook(book);
                completed++;

                setExportProgress(prev => ({
                    ...prev,
                    completed
                }));
            }
            setSelectedBooks(new Set());
        } catch (error) {
            console.error('Error exporting books:', error);
        } finally {
            setIsExporting(false);
            setExportProgress({
                currentBook: '',
                currentStep: '',
                completed: 0
            });
        }
    };

    if (error) {
        return (
            <div className="flex flex-col items-center justify-center h-full space-y-6 p-4">
                <Alert variant="destructive" className="max-w-md flex space-x-2 p-2">
                    <AlertCircle className="w-4" />
                    <AlertDescription className="text-md">{error}</AlertDescription>
                </Alert>
                <Button onClick={loadBooks} variant="outline" disabled={retryCount < maxRetries}>
                    <Loader2 className="mr-2 h-4 w-4" />
                    {retryCount < maxRetries ? (
                        <span className="ml-1">Retrying... ({retryCount}/{maxRetries})</span>
                    ) : (
                        'Retry'
                    )}
                </Button>
            </div>
        );
    }

    if (isLoading) {
        return (
            <div className="flex flex-col items-center justify-center h-full space-y-4">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
                <p className="text-lg text-muted-foreground">Loading books...</p>
            </div>
        );
    }

    return (
        <div className="pb-16 relative">
            <div className="flex justify-between items-center p-4 pb-0">
                <h1 className="text-2xl font-bold">Your Books</h1>
            </div>
            <div className="relative">
                <BookGrid
                    books={books}
                    selectedBooks={selectedBooks}
                    onSelectBook={handleSelectBook}
                />
                <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-t from-background to-transparent pointer-events-none" />
            </div>
            <Footer
                selectedCount={selectedBooks.size}
                totalSelected={selectedBooks.size}
                isExporting={isExporting}
                currentBook={exportProgress.currentBook}
                currentStep={exportProgress.currentStep}
                onExport={handleExport}
            />
        </div>
    );
} 