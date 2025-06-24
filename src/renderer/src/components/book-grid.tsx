import { useState, useEffect } from "react";
import { Book } from "../../../backend/models";
import { Card, CardContent, CardFooter } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { useNetworkState } from "@uidotdev/usehooks";

interface BookGridProps {
  books: Book[];
  selectedBooks: Set<string>;
  onSelectBook: (bookTitle: string) => void;
  isProcessing: boolean;
  currentBook: string;
  exportedBooks: Set<string>;
}

interface BookCardProps extends Book {
  isSelected: boolean;
  onSelect: () => void;
  isProcessing: boolean;
  isExporting: boolean;
  isExported: boolean;
}

function BookCard({
  bookTitle,
  subtitle,
  author,
  readPercent,
  isbn,
  imageId,
  isSelected,
  onSelect,
  isProcessing,
  isExporting,
  isExported,
}: BookCardProps) {
  const [coverUrl, setCoverUrl] = useState<string>("");
  const [progress, setProgress] = useState<number>(Math.round(readPercent));
  const [hasAttemptedLoad, setHasAttemptedLoad] = useState(false);
  const networkState = useNetworkState();

  // Check if book has no progress
  const hasNoProgress = readPercent === 0;
  const isDisabled = hasNoProgress || isProcessing;

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
  }, [bookTitle, isbn, networkState.online, hasAttemptedLoad]);

  const renderCover = () => {
    if (coverUrl) {
      return (
        <img
          src={coverUrl}
          alt={`${bookTitle} cover`}
          className={cn(
            "object-cover w-full h-full select-none",
            hasNoProgress && "opacity-50 grayscale"
          )}
          draggable="false"
        />
      );
    }

    if (!networkState.online && !hasAttemptedLoad) {
      return <Skeleton className="w-full h-full" />;
    }

    return (
      <div className={cn(
        "w-full h-full bg-muted flex items-center justify-center",
        hasNoProgress && "opacity-50"
      )}>
        <span className="text-muted-foreground">No cover</span>
      </div>
    );
  };

  return (
    <div
      className={cn(
        "relative rounded-lg h-full",
        "before:absolute before:inset-0 before:rounded-lg before:transition-all",
        "before:pointer-events-none",
        hasNoProgress && "opacity-60",
        isSelected && !isExporting && !hasNoProgress
          ? "before:border-2 before:border-primary before:-m-[2px]"
          : "before:border before:border-border",
        !hasNoProgress && !isDisabled && "hover:before:border-primary",
        isExporting && "before:animate-border-breathing before:-m-[2px]",
        isExported && !isExporting && "before:border-2 before:border-green-500 before:-m-[2px]",
      )}
    >
      {hasNoProgress ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <Card
              className={cn(
                "flex flex-col overflow-hidden rounded-lg h-full",
                isDisabled ? "cursor-not-allowed" : "cursor-pointer",
                hasNoProgress && "text-muted-foreground"
              )}
              onClick={isDisabled ? undefined : onSelect}
            >
              <div className="relative aspect-[3/4] w-full p-4">{renderCover()}</div>
              <CardContent className="flex-1 p-4 pt-0 pb-4">
                <div className="space-y-1">
                  <h3 className={cn(
                    "font-bold line-clamp-2",
                    hasNoProgress && "text-muted-foreground"
                  )}>{bookTitle}</h3>
                  <p title={author} className="text-sm text-muted-foreground">
                    {(() => {
                      const authorsArray = author.split(", ");
                      const firstThreeAuthors = authorsArray.slice(0, 3).join(", ");
                      const remainingAuthors =
                        authorsArray.slice(3).length > 0
                          ? `, ${authorsArray.slice(3).length} more`
                          : "";
                      return `${firstThreeAuthors}${remainingAuthors}`;
                    })()}
                  </p>
                </div>
              </CardContent>
              <CardFooter className="p-4 pt-0 mt-auto shrink-0">
                <div className="w-full flex items-center gap-2">
                  <span className="text-sm text-muted-foreground">Read</span>
                  <Progress
                    value={progress}
                    className={cn(
                      "h-1",
                      hasNoProgress && "opacity-50"
                    )}
                  />
                  <span className="text-sm text-muted-foreground">
                    {Math.round(progress)}%
                  </span>
                </div>
              </CardFooter>
            </Card>
          </TooltipTrigger>
          <TooltipContent side="bottom">
            <p>No bookmarks found (｡ŏ_ŏ)</p>
          </TooltipContent>
        </Tooltip>
      ) : (
        <Card
          className={cn(
            "flex flex-col overflow-hidden rounded-lg h-full",
            isDisabled ? "cursor-not-allowed" : "cursor-pointer",
            hasNoProgress && "text-muted-foreground"
          )}
          onClick={isDisabled ? undefined : onSelect}
        >
          <div className="relative aspect-[3/4] w-full p-4">{renderCover()}</div>
          <CardContent className="flex-1 p-4 pt-0 pb-4">
            <div className="space-y-1">
              <h3 className={cn(
                "font-bold line-clamp-2",
                hasNoProgress && "text-muted-foreground"
              )}>{bookTitle}</h3>
              {/* {subtitle && (
                            <p className="text-sm text-muted-foreground line-clamp-2">
                                {subtitle}
                            </p>
                        )} */}
              {/* Hide the authors after the third person with "..." */}
              <p title={author} className="text-sm text-muted-foreground">
                {(() => {
                  const authorsArray = author.split(", ");
                  const firstThreeAuthors = authorsArray.slice(0, 3).join(", ");
                  const remainingAuthors =
                    authorsArray.slice(3).length > 0
                      ? `, ${authorsArray.slice(3).length} more`
                      : "";
                  return `${firstThreeAuthors}${remainingAuthors}`;
                })()}
              </p>
            </div>
          </CardContent>
          <CardFooter className="p-4 pt-0 mt-auto shrink-0">
            <div className="w-full flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Read</span>
              <Progress
                value={progress}
                className={cn(
                  "h-1",
                  hasNoProgress && "opacity-50"
                )}
              />
              <span className="text-sm text-muted-foreground">
                {Math.round(progress)}%
              </span>
            </div>
          </CardFooter>
        </Card>
      )}
    </div>
  );
}

export function BookGrid({
  books,
  selectedBooks,
  onSelectBook,
  isProcessing,
  currentBook,
  exportedBooks,
}: BookGridProps) {
  return (
    <TooltipProvider delayDuration={0}>
      <ScrollArea className="h-[calc(100vh-8rem)]">
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-6 p-4 auto-rows-fr mb-16">
          {books.map((book) => (
            <BookCard
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
    </TooltipProvider>
  );
}
