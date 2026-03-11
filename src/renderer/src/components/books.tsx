import { useState, useEffect, useMemo } from "react";
import { useToast } from "@/hooks/use-toast";
import { Book, BookSource } from "../../../backend/models";
import { Footer } from "@/components/footer";
import { ConfirmOverwriteDialog } from "./confirm-overwrite-dialog";
import { DeletePagesDialog } from "./delete-pages-dialog";
import { useBookExport } from "@/hooks/use-book-export";
import { ErrorDisplay } from "./books/error-display";
import { LoadingDisplay } from "./books/loading-display";
import { Header } from "./books/header";
import { BookDisplay } from "./books/book-display";
import { BookmarksPreviewDialog } from "./bookmarks-preview-dialog";

interface BooksProps {
  onExportStateChange?: (exporting: boolean, canceling: boolean, checking: boolean) => void;
}

export function Books({ onExportStateChange }: BooksProps) {
  // Book data states
  const [books, setBooks] = useState<Book[]>([]);
  const [selectedBooks, setSelectedBooks] = useState<Set<string>>(new Set());
  const [exportedBooks, setExportedBooks] = useState<Set<string>>(new Set());

  // UI states
  const [isGridView, setIsGridView] = useState(true);
  const [selectAll, setSelectAll] = useState(false);
  const [sourceFilter, setSourceFilter] = useState<Set<BookSource>>(
    new Set(['kobo-store', 'external'])
  );
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retryCount, setRetryCount] = useState(0);

  // Preview dialog states
  const [isPreviewOpen, setIsPreviewOpen] = useState(false);
  const [previewBookTitle, setPreviewBookTitle] = useState<string | null>(null);

  // Export states and handlers
  const {
    isExporting,
    isCanceling,
    isChecking,
    exportProgress,
    handleExport,
    handleCancel,
    handleOverwriteConfirm,
    handleOverwriteCancel,
    handleDeleteConfirm,
    existingPages,
    uploadedPages,
    showDeleteDialog,
    showOverwriteDialog,
    setShowDeleteDialog,
    setShowOverwriteDialog,
  } = useBookExport({
    books,
    selectedBooks,
    setSelectedBooks,
    exportedBooks,
    setExportedBooks,
    onExportStateChange,
  });

  // Constants
  const maxRetries = 3;
  const retryInterval = 5000;
  const minLoadingTime = 300;

  // Toast
  const { toast } = useToast();

  const filteredBooks = useMemo(
    () => books.filter(book => sourceFilter.has(book.source)),
    [books, sourceFilter]
  );


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
    const selectableBooks = filteredBooks.filter((book) => book.readPercent > 0);
    if (selectAll) {
      const allBookTitles = selectableBooks.map((book) => book.bookTitle);
      setSelectedBooks(new Set(allBookTitles));
    } else {
      if (selectedBooks.size === selectableBooks.length && selectableBooks.length > 0) {
        setSelectedBooks(new Set());
      }
    }
  }, [selectAll, filteredBooks]);

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
      setError("Failed to load the books.\nPlease ensure the kobo is connected and try again.");
    } finally {
      setTimeout(() => setIsLoading(false), minLoadingTime);
    }
  };

  const handleSelectBook = (bookTitle: string) => {
    const selectableBookCount = filteredBooks.filter((book) => book.readPercent > 0).length;
    setSelectedBooks((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(bookTitle)) {
        newSet.delete(bookTitle);
        if (newSet.size < selectableBookCount) setSelectAll(false);
      } else {
        newSet.add(bookTitle);
        if (newSet.size === selectableBookCount) setSelectAll(true);
      }
      return newSet;
    });
  };

  const handlePreviewBookmarks = (bookTitle: string) => {
    setPreviewBookTitle(bookTitle);
    setIsPreviewOpen(true);
  };

  if (error) {
    return <ErrorDisplay error={error} onRetry={loadBooks} retryCount={retryCount} maxRetries={maxRetries} />;
  }

  if (isLoading) {
    return <LoadingDisplay />;
  }

  return (
    <>
      <div className="pb-16 relative">
        <Header
          selectAll={selectAll}
          setSelectAll={setSelectAll}
          isGridView={isGridView}
          setIsGridView={setIsGridView}
          isDisabled={isExporting || isCanceling || isChecking}
          sourceFilter={sourceFilter}
          setSourceFilter={setSourceFilter}
        />
        <BookDisplay
          isGridView={isGridView}
          books={filteredBooks}
          selectedBooks={selectedBooks}
          onSelectBook={handleSelectBook}
          onPreviewBookmarks={handlePreviewBookmarks}
          isProcessing={isExporting || isCanceling || isChecking}
          currentBook={exportProgress.currentBook}
          exportedBooks={exportedBooks}
        />
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

      <DeletePagesDialog
        open={showDeleteDialog}
        onOpenChange={setShowDeleteDialog}
        uploadedPages={uploadedPages}
        onConfirm={handleDeleteConfirm}
        onCancel={() => setShowDeleteDialog(false)}
      />

      <ConfirmOverwriteDialog
        existingPages={existingPages}
        open={showOverwriteDialog}
        onOpenChange={setShowOverwriteDialog}
        onConfirm={handleOverwriteConfirm}
        onCancel={handleOverwriteCancel}
      />

      <BookmarksPreviewDialog
        open={isPreviewOpen}
        onOpenChange={setIsPreviewOpen}
        bookTitle={previewBookTitle}
      />
    </>
  );
}
