import { useState, useEffect } from "react";
import { Book } from "../../../backend/models";
import { Card, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { useNetworkState } from "@uidotdev/usehooks";
import { ScrollArea } from "@/components/ui/scroll-area";

interface BookListProps {
  books: Book[];
  selectedBooks: Set<string>;
  onSelectBook: (bookTitle: string) => void;
  isProcessing: boolean;
  currentBook: string;
  exportedBooks: Set<string>;
}

interface BookListCardProps extends Book {
  isSelected: boolean;
  onSelect: () => void;
  isProcessing: boolean;
  isExporting: boolean;
  isExported: boolean;
}

export function BookListCard({
  bookTitle,
  author,
  readPercent,
  imageId,
  isSelected,
  onSelect,
  isProcessing,
  isExporting,
  isExported,
}: BookListCardProps) {
  const [coverUrl, setCoverUrl] = useState<string>("");
  const [hasAttemptedLoad, setHasAttemptedLoad] = useState(false);
  const networkState = useNetworkState();

  useEffect(() => {
    const loadCover = async () => {
      try {
        if (!imageId) {
          console.warn(`No image ID found for book: ${bookTitle}`);
          return;
        }
        const url = await window.api.fetchBookCover(imageId);
        setCoverUrl(url);
      } catch (error) {
        console.error("Error loading book cover:", error);
      } finally {
        setHasAttemptedLoad(true);
      }
    };

    if (networkState.online && !hasAttemptedLoad) {
      loadCover();
    }
  }, [bookTitle, imageId, networkState.online, hasAttemptedLoad]);

  const renderCover = () => {
    if (coverUrl) {
      return (
        <img
          src={coverUrl}
          alt={`${bookTitle} cover`}
          className="object-cover w-full h-full select-none"
          draggable="false"
        />
      );
    }

    if (!networkState.online && !hasAttemptedLoad) {
      return <Skeleton className="w-full h-full" />;
    }

    return (
      <div className="w-full h-full bg-muted flex items-center justify-center">
        <span className="text-muted-foreground">No cover</span>
      </div>
    );
  };

  return (
    <div
      className={cn(
        "relative rounded-lg",
        "before:absolute before:inset-0 before:rounded-lg before:transition-all",
        "before:pointer-events-none",
        isSelected && !isExporting
          ? "before:border-2 before:border-primary before:-m-[2px]"
          : "before:border before:border-border hover:before:border-primary",
        isExporting && "before:animate-border-breathing before:-m-[2px]",
        isExported && !isExporting && "before:border-2 before:border-green-500 before:-m-[2px]",
      )}
    >
      <Card
        className={cn(
          "flex overflow-hidden rounded-lg",
          isProcessing ? "cursor-not-allowed" : "cursor-pointer"
        )}
        onClick={isProcessing ? undefined : onSelect}
      >
        <div className="aspect-[3/4] h-20 p-2">{renderCover()}</div>
        <CardContent className="flex items-center w-full pl-2 pr-4">
          <div className="flex-1">
            <div className="space-y-1">
              <div className="flex items-center space-x-2">
                <h3 className="font-bold line-clamp-1">{bookTitle}</h3>
                <p className="text-sm text-muted-foreground">
                  {(() => {
                    const authorsArray = author.split(", ");
                    const firstThreeAuthors = authorsArray
                      .slice(0, 3)
                      .join(", ");
                    const remainingAuthors =
                      authorsArray.slice(3).length > 0
                        ? `, ${authorsArray.slice(3).length} more`
                        : "";
                    return `${firstThreeAuthors}${remainingAuthors}`;
                  })()}
                </p>
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2 w-1/4 ml-4">
            <span className="text-sm text-muted-foreground">Read</span>
            <Progress value={Math.round(readPercent)} className="h-1" />
            <span className="text-sm text-muted-foreground w-12 text-right">
              {Math.round(readPercent)}%
            </span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

export function BookList({
  books,
  selectedBooks,
  onSelectBook,
  isProcessing,
  currentBook,
  exportedBooks,
}: BookListProps) {
  return (
    <ScrollArea className="h-[calc(100vh-8rem)]">
      <div className="space-y-2 p-4 mb-12">
        {books.map((book) => (
          <BookListCard
            key={book.bookTitle}
            {...book}
            isSelected={selectedBooks.has(book.bookTitle)}
            onSelect={() => onSelectBook(book.bookTitle)}
            isProcessing={isProcessing}
            isExporting={currentBook === book.bookTitle}
            isExported={exportedBooks.has(book.bookTitle)}
          />
        ))}
      </div>
    </ScrollArea>
  );
}
