import { useState, useEffect } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Bookmark } from "../../../backend/models";

interface BookmarksPreviewDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bookTitle: string | null;
}

export function BookmarksPreviewDialog({
  open,
  onOpenChange,
  bookTitle,
}: BookmarksPreviewDialogProps): React.JSX.Element {
  const [bookmarks, setBookmarks] = useState<Bookmark[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (open && bookTitle) {
      const fetchBookmarks = async (): Promise<void> => {
        setIsLoading(true);
        setError(null);
        try {
          const fetchedBookmarks = await window.api.getBookmarks(bookTitle);
          setBookmarks(fetchedBookmarks);
        } catch (err) {
          console.error("Failed to fetch bookmarks:", err);
          setError("Could not load bookmarks for this book.");
        } finally {
          setIsLoading(false);
        }
      };
      fetchBookmarks();
    }
  }, [open, bookTitle]);

  const renderContent = (): React.JSX.Element => {
    if (isLoading) {
      return (
        <div className="space-y-4">
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
        </div>
      );
    }

    if (error) {
      return <p className="text-destructive">{error}</p>;
    }

    if (bookmarks.length === 0) {
      return <p>No bookmarks found for this book.</p>;
    }

    const hasAnyAnnotation = bookmarks.some(
      (b) => b.annotation && b.annotation.trim() !== "",
    );
    // Make the table selectable by wrapping it in a div with select-text
    return (
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[50px] text-center">#</TableHead>
            <TableHead className={hasAnyAnnotation ? "w-3/5" : "w-full"}>
              Bookmark
            </TableHead>
            {hasAnyAnnotation && <TableHead className="w-2/5">Note</TableHead>}
          </TableRow>
        </TableHeader>
        <TableBody>
          {bookmarks.map((bookmark, index) => (
            <TableRow key={index}>
              <TableCell className="font-medium text-center">
                {index + 1}
              </TableCell>
              <TableCell className="select-text">
                {bookmark.highlight}
              </TableCell>
              {hasAnyAnnotation && (
                <TableCell className="select-text">
                  {bookmark.annotation}
                </TableCell>
              )}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    );
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[800px]">
        <DialogHeader>
          <DialogTitle>{bookTitle}</DialogTitle>
        </DialogHeader>
        <ScrollArea className="h-96 pr-6">{renderContent()}</ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
