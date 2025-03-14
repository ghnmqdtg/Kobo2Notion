import { useState, useRef } from "react";
import { useToast } from "./use-toast";
import { Book } from "../../../backend/models";
import { ExportProgress, ExistingPage, UploadedPage } from "@/types";

interface UseBookExportProps {
  books: Book[];
  selectedBooks: Set<string>;
  setSelectedBooks: (books: Set<string>) => void;
  exportedBooks: Set<string>;
  setExportedBooks: (
    books: Set<string> | ((prev: Set<string>) => Set<string>),
  ) => void;
  onExportStateChange?: (
    exporting: boolean,
    canceling: boolean,
    checking: boolean,
  ) => void;
}

interface UseBookExportReturn {
  isExporting: boolean;
  isCanceling: boolean;
  isChecking: boolean;
  exportProgress: ExportProgress;
  startExport: () => Promise<void>;
  handleExport: () => Promise<void>;
  handleCancel: () => void;
  handleOverwriteConfirm: (
    selectedPageIds: string[],
    skippedBooks: string[],
  ) => Promise<void>;
  handleOverwriteCancel: () => void;
  handleDeleteConfirm: () => Promise<void>;
  existingPages: ExistingPage[];
  uploadedPages: UploadedPage[];
  showDeleteDialog: boolean;
  showOverwriteDialog: boolean;
  setShowDeleteDialog: (show: boolean) => void;
  setShowOverwriteDialog: (show: boolean) => void;
}

export function useBookExport({
  books,
  selectedBooks,
  setSelectedBooks,
  exportedBooks,
  setExportedBooks,
  onExportStateChange,
}: UseBookExportProps): UseBookExportReturn {
  const [isExporting, setIsExporting] = useState(false);
  const [isCanceling, setIsCanceling] = useState(false);
  const [isChecking, setIsChecking] = useState(false);
  const [exportProgress, setExportProgress] = useState<ExportProgress>({
    currentBook: "",
    currentStep: "",
    completed: 0,
  });

  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [showOverwriteDialog, setShowOverwriteDialog] = useState(false);
  const [existingPages, setExistingPages] = useState<ExistingPage[]>([]);
  const [uploadedPages, setUploadedPages] = useState<UploadedPage[]>([]);

  const cancelRef = useRef(false);
  const { toast } = useToast();

  const updateStates = (
    exporting: boolean,
    canceling: boolean,
    checking: boolean,
  ) => {
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
      updateStates(false, false, false);
    }
  };

  const startExport = async (skippedBooks: string[] = []): Promise<void> => {
    setUploadedPages([]);
    setExportedBooks(new Set());
    let completed = 0;
    let remainingBooks: string[] = [];

    // Copy the selectedBooks because we failed update the state here
    const currentSelected = Array.from(selectedBooks);

    // Check if there are any books to skip
    if (skippedBooks.length > 0) {
      remainingBooks = currentSelected.filter(
        (book) => !skippedBooks.includes(book),
      );
      setSelectedBooks(new Set(remainingBooks));
    } else {
      remainingBooks = currentSelected;
    }

    try {
      for (const bookTitle of remainingBooks) {
        if (cancelRef.current) break;

        const book = books.find((b) => b.bookTitle === bookTitle);
        if (!book) continue;

        setExportProgress({
          currentBook: book.bookTitle,
          currentStep: "Exporting highlights...",
          completed,
        });

        const { parentPageId } = await window.api.exportBook(book);

        setExportedBooks((prev) => new Set([...prev, book.bookTitle]));
        setUploadedPages((prev) => [
          ...prev,
          { pageId: parentPageId, bookTitle: book.bookTitle },
        ]);

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
      if (cancelRef.current) {
        setExportedBooks(new Set());
      }
      updateStates(false, false, false);
      setExportProgress({
        currentBook: "",
        currentStep: "",
        completed: 0,
      });
      cancelRef.current = false;
    }
  };

  const handleCancel = () => {
    cancelRef.current = true;
    updateStates(true, true, false);
  };

  const handleOverwriteConfirm = async (
    selectedPageIds: string[],
    skippedBooks: string[],
  ) => {
    setShowOverwriteDialog(false);

    console.log(selectedPageIds, skippedBooks);

    try {
      // Delete the old pages
      await Promise.all(
        selectedPageIds.map(async (pageId) => {
          const result = await window.api.deleteNotionPage(pageId);
          if (!result.success) {
            throw new Error(result.message);
          }
        }),
      );

      updateStates(true, false, false); // Start export after deletion
      await startExport(skippedBooks);
    } catch (error) {
      console.error("Error deleting pages:", error);
      toast({
        title: "Error",
        description: "Failed to delete existing pages",
        variant: "destructive",
      });
      updateStates(false, false, false);
    }
  };

  const handleOverwriteCancel = () => {
    setShowOverwriteDialog(false);
    setExistingPages([]);
    setExportedBooks(new Set());
    updateStates(false, false, false);
  };

  const handleDeleteConfirm = async () => {
    if (uploadedPages.length > 0) {
      try {
        await Promise.all(
          uploadedPages.map(async ({ pageId, bookTitle }) => {
            try {
              await window.api
                .deleteNotionPage(pageId)
                .then(({ success, message }) => {
                  if (success) {
                    console.log(`Deleted page for book: ${bookTitle}`);
                  } else {
                    console.error(
                      `Failed to delete page for book: ${bookTitle}`,
                      message,
                    );
                    throw new Error(message);
                  }
                });
            } catch (error) {
              console.error(
                `Failed to delete page for book: ${bookTitle}`,
                error,
              );
              throw error;
            }
          }),
        );

        toast({
          title: "Pages Deleted",
          description: `Removed the following pages from Notion:\n${uploadedPages.map((page) => `• ${page.bookTitle}`).join("\n")}`,
          variant: "default",
        });
      } catch (error) {
        console.error("Error deleting pages:", error);
        toast({
          title: "Error",
          description: `Failed to delete pages from Notion.`,
          variant: "destructive",
        });
      }
    }
    setShowDeleteDialog(false);
    setUploadedPages([]);
    setExportedBooks(new Set());
    updateStates(false, false, false);
  };

  return {
    isExporting,
    isCanceling,
    isChecking,
    exportProgress,
    startExport: () => startExport([]),
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
  };
}
