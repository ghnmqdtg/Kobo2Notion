import { useState, useEffect } from 'react';
import { BookGrid } from '@/components/book-grid';
import { BookList } from './book-list';
import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, AlertCircle, CheckSquare, Bold, Underline, Italic, LayoutGrid, List } from 'lucide-react';
import { Toggle } from '@/components/ui/toggle';
import { Footer } from '@/components/footer';
import { Book } from '../../../backend/models';
import { useToast } from "@/hooks/use-toast";
import { ToastAction } from "@/components/ui/toast";

export function Books() {
    const [books, setBooks] = useState<Book[]>([]);
    const [selectedBooks, setSelectedBooks] = useState<Set<string>>(new Set());
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [isExporting, setIsExporting] = useState(false);
    const [minLoadingTime] = useState(300); // 0.3 second minimum loading time
    const [retryCount, setRetryCount] = useState(0);
    const [exportProgress, setExportProgress] = useState({
        currentBook: '',
        currentStep: '',
        completed: 0
    });
    const [selectAll, setSelectAll] = useState(false);
    const [isGridView, setIsGridView] = useState(true);
    const { toast } = useToast();

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

    useEffect(() => {
        if (selectAll) {
            const allBookTitles = books.map(book => book.bookTitle);
            setSelectedBooks(new Set(allBookTitles));
        } else {
            if (selectedBooks.size === books.length) {
                setSelectedBooks(new Set());
            }
        }
    }, [selectAll, books]);

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
            setError('Failed to load the books.\nPlease ensure the kobo is connected and try again.');
        } finally {
            setTimeout(() => {
                setIsLoading(false);
            }, minLoadingTime);
        }
    };

    const handleSelectBook = (bookTitle: string) => {
        setSelectedBooks((prev) => {
            const newSet = new Set(prev);
            if (newSet.has(bookTitle)) {
                newSet.delete(bookTitle);
                if (newSet.size < books.length) {
                    setSelectAll(false);
                }
            } else {
                newSet.add(bookTitle);
                if (newSet.size === books.length) {
                    setSelectAll(true);
                }
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

                // First step: Exporting highlights
                setExportProgress({
                    currentBook: book.bookTitle,
                    currentStep: 'Exporting highlights...',
                    completed
                });

                const { parentPageId, highlightPageId } = await window.api.exportBook(book);

                // Second step: Summarizing (if enabled)
                if (window.env.SUMMARIZE_ENABLED) {
                    setExportProgress(prev => ({
                        ...prev,
                        currentStep: 'Summarizing highlights...'
                    }));

                    await window.api.summarizeBook(book, parentPageId);
                }

                completed++;
                setExportProgress(prev => ({
                    ...prev,
                    completed,
                    currentStep: ''
                }));
            }
            setSelectedBooks(new Set());
        } catch (error) {
            console.error('Error exporting books:', error);
            toast({
                title: 'Error exporting books',
                description: 'Please check the Kobo is connected.',
                action: <ToastAction onClick={loadBooks} altText="Try reloading">Try again</ToastAction>
            });
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
            <div className="flex flex-col items-center justify-center h-full space-y-6">
                <Alert variant="destructive" className="max-w-lg rounded-md">
                    <div className="flex items-center space-x-3">
                        <AlertCircle className="h-6 w-6" />
                        <AlertDescription className="text-lg whitespace-pre-line">
                            {error}
                        </AlertDescription>
                    </div>
                </Alert>
                <Button
                    className="text-md font-bold"
                    onClick={loadBooks}
                    variant="outline"
                    disabled={retryCount < maxRetries}
                >
                    {retryCount < maxRetries ? (
                        <>
                            <Loader2 className="h-4 w-4 animate-spin" />
                            <span className="ml-1">Retrying... ({retryCount}/{maxRetries})</span>
                        </>
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

                <div className="flex items-center gap-2">
                    <Toggle
                        pressed={selectAll}
                        onPressedChange={setSelectAll}
                        aria-label="Toggle select all"
                    >
                        <CheckSquare className="h-4 w-4" />
                        <span>Select all</span>
                    </Toggle>

                    <Toggle
                        pressed={isGridView}
                        onPressedChange={setIsGridView}
                        aria-label="Toggle view"
                    >
                        {isGridView ? (
                            <>
                                <List className="h-4 w-4" />
                                <span>List view</span>
                            </>
                        ) : (
                            <>
                                <LayoutGrid className="h-4 w-4" />
                                <span>Grid view</span>
                            </>
                        )}
                    </Toggle>
                </div>

            </div>
            <div className="relative">
                {isGridView ? (
                    <BookGrid
                        books={books}
                        selectedBooks={selectedBooks}
                        onSelectBook={handleSelectBook}
                    />
                ) : (
                    <BookList
                        books={books}
                        selectedBooks={selectedBooks}
                        onSelectBook={handleSelectBook}
                    />
                )}
                <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-t from-background to-transparent pointer-events-none" />
            </div>
            <Footer
                selectedCount={selectedBooks.size}
                isExporting={isExporting}
                currentBook={exportProgress.currentBook}
                currentStep={exportProgress.currentStep}
                completed={exportProgress.completed}
                onExport={handleExport}
            />
        </div>
    );
} 