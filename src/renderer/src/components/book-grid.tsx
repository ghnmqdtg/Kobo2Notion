import React from 'react'
import { Book } from '../../../backend/models'
import { Card, CardContent, CardFooter } from '@/components/ui/card'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn, formatAuthors } from '@/lib/utils'
import { Button } from './ui/button'
import { Separator } from './ui/separator'
import { useBookCover } from '@/hooks/use-book-cover'

interface BookGridProps {
  books: Book[]
  selectedBooks: Set<string>
  onSelectBook: (bookTitle: string) => void
  onPreviewBookmarks: (bookTitle: string) => void
  isProcessing: boolean
  currentBook: string
  exportedBooks: Set<string>
}

interface BookCardProps extends Book {
  isSelected: boolean
  onSelect: () => void
  onPreview: () => void
  isProcessing: boolean
  isExporting: boolean
  isExported: boolean
}

const sourceLabel: Record<string, string> = {
  'kobo-store': 'Kobo',
  external: 'External'
}

const BookCard = React.memo(function BookCard({
  bookTitle,
  author,
  readPercent,
  imageId,
  bookmarkCount,
  source,
  contentType,
  isSelected,
  onSelect,
  onPreview,
  isProcessing,
  isExporting,
  isExported
}: BookCardProps): React.JSX.Element {
  const { coverDataUrl, isLoading: isCoverLoading } = useBookCover(imageId)

  // Check if book has no progress
  const hasNoProgress = readPercent === 0
  const isPdfNoBookmarks = contentType === 'pdf' && (!bookmarkCount || bookmarkCount <= 0)
  const isDisabled = hasNoProgress || isPdfNoBookmarks || isProcessing

  const renderCover = (): React.JSX.Element => {
    if (isCoverLoading) {
      return <Skeleton className="w-full h-full" />
    }

    if (coverDataUrl) {
      return (
        <img
          src={coverDataUrl}
          alt={`${bookTitle} cover`}
          className={cn(
            'object-cover w-full h-full select-none',
            hasNoProgress && 'opacity-50 grayscale'
          )}
          draggable="false"
        />
      )
    }

    return (
      <div
        className={cn(
          'w-full h-full bg-muted flex items-center justify-center',
          hasNoProgress && 'opacity-50'
        )}
      >
        <span className="text-muted-foreground">No cover</span>
      </div>
    )
  }

  return (
    <div
      className={cn(
        'relative rounded-lg h-full',
        'before:absolute before:inset-0 before:rounded-lg before:transition-all',
        'before:pointer-events-none',
        (hasNoProgress || isPdfNoBookmarks) && 'opacity-60',
        isSelected && !isExporting && !hasNoProgress
          ? 'before:border-2 before:border-primary before:-m-[2px]'
          : 'before:border before:border-border',
        !hasNoProgress && !isDisabled && 'hover:before:border-primary',
        isExporting && 'before:animate-border-breathing before:-m-[2px]',
        isExported && !isExporting && 'before:border-2 before:border-green-500 before:-m-[2px]'
      )}
    >
      {hasNoProgress || isPdfNoBookmarks ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <Card
              className={cn(
                'flex flex-col overflow-hidden rounded-lg h-full',
                isDisabled ? 'cursor-not-allowed' : 'cursor-pointer',
                (hasNoProgress || isPdfNoBookmarks) && 'text-muted-foreground'
              )}
              onClick={isDisabled ? undefined : onSelect}
            >
              <div className="relative aspect-[3/4] w-full p-4">
                {renderCover()}
                <div className="absolute top-5 right-5 flex items-center gap-1">
                  {source !== 'kobo-store' && (
                    <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wide bg-black/60 backdrop-blur-sm text-white">
                      {sourceLabel[source]}
                    </span>
                  )}
                  {contentType && (
                    <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wide bg-black/60 backdrop-blur-sm text-white">
                      {contentType}
                    </span>
                  )}
                </div>
              </div>
              <CardContent className="flex-1 p-4 pt-0 pb-4">
                <div className="space-y-1">
                  <h3
                    className={cn(
                      'font-bold line-clamp-2',
                      (hasNoProgress || isPdfNoBookmarks) && 'text-muted-foreground'
                    )}
                  >
                    {bookTitle}
                  </h3>
                  <p title={author} className="text-sm text-muted-foreground">
                    {formatAuthors(author)}
                  </p>
                </div>
              </CardContent>
              <CardFooter className="flex items-center p-4 pt-0 mt-auto text-sm text-muted-foreground">
                <Button
                  variant="ghost"
                  className="flex items-center w-full h-auto p-0 text-sm font-normal text-muted-foreground hover:text-foreground disabled:opacity-50"
                  onClick={(e) => {
                    e.stopPropagation()
                    onPreview()
                  }}
                  disabled={!bookmarkCount || bookmarkCount <= 0}
                >
                  <div className="flex-1 text-center">{bookmarkCount ?? 0} notes</div>
                  <Separator orientation="vertical" className="h-4 mx-2" />
                  <div className="flex-1 text-center">{Math.round(readPercent)}%</div>
                </Button>
              </CardFooter>
            </Card>
          </TooltipTrigger>
          <TooltipContent side="bottom">
            <p>
              {isPdfNoBookmarks
                ? "PDF books don't support highlights"
                : 'No bookmarks found (｡ŏ_ŏ)'}
            </p>
          </TooltipContent>
        </Tooltip>
      ) : (
        <Card
          className={cn(
            'flex flex-col overflow-hidden rounded-lg h-full',
            isDisabled ? 'cursor-not-allowed' : 'cursor-pointer'
          )}
          onClick={isDisabled ? undefined : onSelect}
        >
          <div className="relative aspect-[3/4] w-full p-4">
            {renderCover()}
            <div className="absolute top-5 right-5 flex items-center gap-1">
              {source !== 'kobo-store' && (
                <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wide bg-black/60 backdrop-blur-sm text-white">
                  {sourceLabel[source]}
                </span>
              )}
              {contentType && (
                <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wide bg-black/60 backdrop-blur-sm text-white">
                  {contentType}
                </span>
              )}
            </div>
          </div>
          <CardContent className="flex-1 p-4 pt-0 pb-4">
            <div className="space-y-1">
              <h3 className={cn('font-bold line-clamp-2')}>{bookTitle}</h3>
              <p title={author} className="text-sm text-muted-foreground">
                {((): string => {
                  const authorsArray = (author || '').split(', ')
                  const firstThreeAuthors = authorsArray.slice(0, 3).join(', ')
                  const remainingAuthors =
                    authorsArray.slice(3).length > 0 ? `, ${authorsArray.slice(3).length} more` : ''
                  return `${firstThreeAuthors}${remainingAuthors}`
                })()}
              </p>
            </div>
          </CardContent>
          <CardFooter className="flex items-center p-4 pt-0 mt-auto text-sm text-muted-foreground">
            <Button
              variant="ghost"
              className="flex items-center w-full h-auto p-0 text-sm font-normal text-muted-foreground hover:text-foreground disabled:opacity-50"
              onClick={(e) => {
                e.stopPropagation()
                onPreview()
              }}
              disabled={!bookmarkCount || bookmarkCount <= 0}
            >
              <div className="flex-1 text-center">{bookmarkCount ?? 0} notes</div>
              <Separator orientation="vertical" className="h-4 mx-2" />
              <div className="flex-1 text-center">{Math.round(readPercent)}%</div>
            </Button>
          </CardFooter>
        </Card>
      )}
    </div>
  )
})

export function BookGrid({
  books,
  selectedBooks,
  onSelectBook,
  onPreviewBookmarks,
  isProcessing,
  currentBook,
  exportedBooks
}: BookGridProps): React.JSX.Element {
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
              onPreview={() => onPreviewBookmarks(book.bookTitle)}
              isProcessing={isProcessing}
              isExporting={currentBook === book.bookTitle}
              isExported={exportedBooks.has(book.bookTitle)}
            />
          ))}
        </div>
      </ScrollArea>
    </TooltipProvider>
  )
}
