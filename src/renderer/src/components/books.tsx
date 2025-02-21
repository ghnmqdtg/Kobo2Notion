import { useState, useEffect, useRef } from "react";
import { BookGrid } from "@/components/book-grid";
import { BookList } from "./book-list";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import {
  Loader2,
  AlertCircle,
  CheckSquare,
  Bold,
  Underline,
  Italic,
  LayoutGrid,
  List,
} from "lucide-react";
import { Toggle } from "@/components/ui/toggle";
import { Footer } from "@/components/footer";
import { Book } from "../../../backend/models";
import { useToast } from "@/hooks/use-toast";
import { ToastAction } from "@/components/ui/toast";
import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogCancel,
  AlertDialogAction,
} from "@/components/ui/alert-dialog";
import { ConfirmOverwriteDialog } from "./confirm-overwrite-dialog";

interface BooksProps {
  onExportStateChange?: (exporting: boolean, canceling: boolean, checking: boolean) => void;
}

export function Books({ onExportStateChange }: BooksProps) {
  // Book data
  const [books, setBooks] = useState<Book[]>([]);
  const [selectedBooks, setSelectedBooks] = useState<Set<string>>(new Set());
  const [exportedBooks, setExportedBooks] = useState<Set<string>>(new Set());

  // UI states
  const [isGridView, setIsGridView] = useState(true);
  const [selectAll, setSelectAll] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [minLoadingTime] = useState(300); // 0.3 second minimum loading time
  const [retryCount, setRetryCount] = useState(0);

  // Export states
  const [isExporting, setIsExporting] = useState(false);
  const [isCanceling, setIsCanceling] = useState(false);
  const [isChecking, setIsChecking] = useState(false);
  const [exportProgress, setExportProgress] = useState({
    currentBook: "",
    currentStep: "",
    completed: 0,
  });
  const cancelRef = useRef(false);

  // Dialog states
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [showOverwriteDialog, setShowOverwriteDialog] = useState(false);
  const [existingPages, setExistingPages] = useState<ExistingPage[]>([]);
  const [uploadedPages, setUploadedPages] = useState<Array<{
    pageId: string;
    bookTitle: string;
  }>>([]);

  // Toast
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
      const allBookTitles = books.map((book) => book.bookTitle);
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
      console.error("Error loading books:", error);
      setError(
        "Failed to load the books.\nPlease ensure the kobo is connected and try again.",
      );
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

  // Helper function to update states and notify parent
  const updateStates = (exporting: boolean, canceling: boolean, checking: boolean) => {
    setIsExporting(exporting);
    setIsCanceling(canceling);
    setIsChecking(checking);
    onExportStateChange?.(exporting, canceling, checking);
  };

  const handleExport = async () => {
    if (selectedBooks.size === 0) return;

    updateStates(false, false, true); // Start checking

    try {
      const bookTitles = Array.from(selectedBooks);
      const existing = await window.api.queryExistingPages(bookTitles);

      if (existing.length > 0) {
        setExistingPages(existing);
        setShowOverwriteDialog(true);
      } else {
        updateStates(true, false, false); // Start export
        await startExport();
      }
    } catch (error) {
      console.error("Error checking existing pages:", error);
      toast({
        title: "Error",
        description: "Failed to check existing pages in Notion",
        variant: "destructive",
      });
      updateStates(false, false, false); // Reset states on error
    }
  };

  const startExport = async () => {
    setUploadedPages([]);
    setExportedBooks(new Set());
    let completed = 0;

    try {
      for (const bookTitle of selectedBooks) {
        if (cancelRef.current) {
          break;
        }

        const book = books.find((b) => b.bookTitle === bookTitle);
        if (!book) continue;

        setExportProgress({
          currentBook: book.bookTitle,
          currentStep: "Exporting highlights...",
          completed,
        });

        const { parentPageId, highlightPageId } = await window.api.exportBook(book);

        setExportedBooks(prev => new Set([...prev, book.bookTitle]));

        setUploadedPages(prev => [...prev, {
          pageId: parentPageId,
          bookTitle: book.bookTitle
        }]);

        if (cancelRef.current) {
          setShowDeleteDialog(true);
          break;
        }

        if (window.env.SUMMARIZE_ENABLED) {
          setExportProgress((prev) => ({
            ...prev,
            currentStep: "Summarizing highlights...",
          }));

          await window.api.summarizeBook(book, parentPageId);

          if (cancelRef.current) {
            setShowDeleteDialog(true);
            break;
          }
        }

        completed++;
        setExportProgress((prev) => ({
          ...prev,
          completed,
          currentStep: "",
        }));
      }

      if (!cancelRef.current) {
        setSelectedBooks(new Set());
        setExportedBooks(new Set());
        setUploadedPages([]);
        toast({
          title: "Export complete",
          description: "Your books have been exported to Notion.",
        });
      }
    } catch (error) {
      console.error("Error exporting books:", error);
      toast({
        title: "Error exporting books",
        description: "Please delete Notion pages and try again.",
        variant: "destructive",
      });
    } finally {
      updateStates(false, false, false); // Reset all states
      setSelectAll(false);
      setExportProgress({
        currentBook: "",
        currentStep: "",
        completed: 0,
      });
      cancelRef.current = false;
      if (!cancelRef.current) {
        setExportedBooks(new Set());
      }
    }
  };

  const handleCancel = () => {
    cancelRef.current = true;
    updateStates(true, true, false); // Update to canceling state
  };

  const handleDeleteConfirm = async () => {
    if (uploadedPages.length > 0) {
      try {
        await Promise.all(
          uploadedPages.map(async ({ pageId, bookTitle }) => {
            try {
              await window.api.deleteNotionPage(pageId).then(({ success, message }) => {
                if (success) {
                  console.log(`Deleted page for book: ${bookTitle}`);
                } else {
                  console.error(`Failed to delete page for book: ${bookTitle}`, message);
                  throw new Error(message);
                }
              });
            } catch (error) {
              console.error(`Failed to delete page for book: ${bookTitle}`, error);
              throw error;
            }
          })
        );

        toast({
          title: "Pages Deleted",
          description: `Removed the following pages from Notion:\n${uploadedPages.map(page => `• ${page.bookTitle}`).join('\n')}`,
          variant: "default",
        });
      } catch (error) {
        console.error("Error deleting pages:", error);
        toast({
          title: "Error",
          description: `Failed to delete the page for ${uploadedPages[0].bookTitle} from Notion.`,
          variant: "destructive",
        });
      }
    }
    setShowDeleteDialog(false);
    setUploadedPages([]);
  };

  const handleOverwriteConfirm = async (selectedPageIds: string[]) => {
    setShowOverwriteDialog(false);

    try {
      // Delete the old pages
      await Promise.all(selectedPageIds.map(async (pageId) => {
        const result = await window.api.deleteNotionPage(pageId);
        if (!result.success) {
          throw new Error(result.message);
        }
      }));

      updateStates(true, false, false); // Start export after deletion
      await startExport();
    } catch (error) {
      console.error("Error deleting pages:", error);
      toast({
        title: "Error",
        description: "Failed to delete existing pages",
        variant: "destructive",
      });
      updateStates(false, false, false); // Reset states on error
    }
  };

  const handleOverwriteCancel = () => {
    setShowOverwriteDialog(false);
    setExistingPages([]);
    updateStates(false, false, false); // Reset all states
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
              <span className="ml-1">
                Retrying... ({retryCount}/{maxRetries})
              </span>
            </>
          ) : (
            "Retry"
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
    <>
      <div className="pb-16 relative">
        <div className="flex justify-between items-center p-4 pb-0">
          <h1 className="text-2xl font-bold">Your Books</h1>

          <div className="flex items-center gap-2">
            <Toggle
              pressed={selectAll}
              onPressedChange={setSelectAll}
              aria-label="Toggle select all"
              disabled={isExporting || isCanceling || isChecking}
            >
              <CheckSquare className="h-4 w-4" />
              <span>Select all</span>
            </Toggle>

            <Toggle
              pressed={isGridView}
              onPressedChange={setIsGridView}
              aria-label="Toggle view"
              className="w-[110px]"
            >
              {isGridView ? (
                <>
                  <LayoutGrid className="h-4 w-4" />
                  <span>Grid view</span>
                </>
              ) : (
                <>
                  <List className="h-4 w-4" />
                  <span>List view</span>
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
              isProcessing={isExporting || isCanceling || isChecking}
              currentBook={exportProgress.currentBook}
              exportedBooks={exportedBooks}
            />
          ) : (
            <BookList
              books={books}
              selectedBooks={selectedBooks}
              onSelectBook={handleSelectBook}
              isProcessing={isExporting || isCanceling || isChecking}
              currentBook={exportProgress.currentBook}
              exportedBooks={exportedBooks}
            />
          )}
          <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-t from-background to-transparent pointer-events-none" />
        </div>
        <Footer
          selectedCount={selectedBooks.size}
          isExporting={isExporting}
          isCanceling={isCanceling}
          isChecking={isChecking}
          currentBook={exportProgress.currentBook}
          currentStep={exportProgress.currentStep}
          completed={exportProgress.completed}
          onExport={handleExport}
          onCancel={handleCancel}
        />
      </div>

      <AlertDialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Uploaded Pages?</AlertDialogTitle>
            <AlertDialogDescription>
              Would you like to remove {uploadedPages.length} partially uploaded page{uploadedPages.length > 1 ? 's' : ''} from Notion?
              {uploadedPages.length > 0 && (
                <ul className="mt-2 space-y-1">
                  {uploadedPages.map(({ bookTitle }) => (
                    <li key={bookTitle} className="text-sm">• {bookTitle}</li>
                  ))}
                </ul>
              )}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => {
              setShowDeleteDialog(false);
              setUploadedPages([]);
            }}>
              Keep Pages
            </AlertDialogCancel>
            <AlertDialogAction onClick={handleDeleteConfirm}>
              Delete Pages
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <ConfirmOverwriteDialog
        existingPages={existingPages}
        open={showOverwriteDialog}
        onOpenChange={setShowOverwriteDialog}
        onConfirm={handleOverwriteConfirm}
        onCancel={handleOverwriteCancel}
      />
    </>
  );
}
