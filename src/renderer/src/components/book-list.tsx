import React from 'react'
import { Book } from '../../../backend/models'
import { Card, CardContent } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn, formatAuthors } from '@/lib/utils'
import { BookOpen } from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Button } from './ui/button'
import { Separator } from './ui/separator'
import { useBookCover } from '@/hooks/use-book-cover'

interface BookListProps {
  books: Book[]
  selectedBooks: Set<string>
  onSelectBook: (bookTitle: string) => void
  onPreviewBookmarks: (bookTitle: string) => void
  isProcessing: boolean
  currentBook: string
  exportedBooks: Set<string>
}

interface BookListCardProps extends Book {
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

export const BookListCard = React.memo(function BookListCard({
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
}: BookListCardProps): React.JSX.Element {
  const { coverDataUrl, isLoading: isCoverLoading } = useBookCover(imageId)

  // Check if book has no progress and no bookmarks
  const hasNoProgressAndNoBookmarks = readPercent === 0 && (!bookmarkCount || bookmarkCount <= 0)
  const isPdfNoBookmarks = contentType === 'pdf' && (!bookmarkCount || bookmarkCount <= 0)
  const isDisabled = hasNoProgressAndNoBookmarks || isPdfNoBookmarks || isProcessing

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
            hasNoProgressAndNoBookmarks && 'opacity-50 grayscale'
          )}
          draggable="false"
        />
      )
    }

    return (
      <div
        className={cn(
          'w-full h-full bg-muted flex items-center justify-center rounded',
          hasNoProgressAndNoBookmarks && 'opacity-50'
        )}
      >
        <BookOpen className="w-5 h-5 text-muted-foreground/60" />
      </div>
    )
  }

  return (
    <div
      className={cn(
        'relative rounded-lg',
        'before:absolute before:inset-0 before:rounded-lg before:transition-all',
        'before:pointer-events-none',
        (hasNoProgressAndNoBookmarks || isPdfNoBookmarks) && 'opacity-60',
        isSelected && !isExporting && !hasNoProgressAndNoBookmarks
          ? 'before:border-2 before:border-primary before:-m-[2px]'
          : 'before:border before:border-border',
        !hasNoProgressAndNoBookmarks && !isDisabled && 'hover:before:border-primary',
        isExporting && 'before:animate-border-breathing before:-m-[2px]',
        isExported && !isExporting && 'before:border-2 before:border-green-500 before:-m-[2px]'
      )}
    >
      {hasNoProgressAndNoBookmarks || isPdfNoBookmarks ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <Card
              className={cn(
                'flex overflow-hidden rounded-lg',
                isDisabled ? 'cursor-not-allowed' : 'cursor-pointer',
                (hasNoProgressAndNoBookmarks || isPdfNoBookmarks) && 'text-muted-foreground'
              )}
              onClick={isDisabled ? undefined : onSelect}
            >
              <div className="relative aspect-[3/4] h-20 p-2">{renderCover()}</div>
              <CardContent className="flex items-center justify-between w-full pl-2 pr-4">
                <div className="flex-1">
                  <div className="space-y-1">
                    <div className="flex items-center space-x-2">
                      <h3
                        className={cn(
                          'font-bold font-mono line-clamp-1',
                          hasNoProgressAndNoBookmarks && 'text-muted-foreground'
                        )}
                      >
                        {bookTitle}
                      </h3>
                      {source !== 'kobo-store' && (
                        <span className="shrink-0 px-1.5 py-0.5 rounded text-[9px] font-semibold uppercase tracking-wide bg-[#64748B] text-white">
                          {sourceLabel[source]}
                        </span>
                      )}
                      {contentType && (
                        <span className="shrink-0 px-1.5 py-0.5 rounded text-[9px] font-semibold uppercase tracking-wide bg-[#1E293B] text-white">
                          {contentType}
                        </span>
                      )}
                      <p title={author} className="text-sm text-muted-foreground">
                        {formatAuthors(author)}
                      </p>
                    </div>
                  </div>
                </div>
                <Button
                  variant="ghost"
                  className="flex items-center w-[35%] h-auto p-0 text-sm font-normal text-muted-foreground hover:text-foreground disabled:opacity-50"
                  onClick={(e) => {
                    e.stopPropagation()
                    onPreview()
                  }}
                  disabled={!bookmarkCount || bookmarkCount <= 0}
                >
                  <div className="w-20 text-right">{bookmarkCount ?? 0} notes</div>
                  <Separator orientation="vertical" className="h-4 mx-2" />
                  <div className="flex-1 flex items-center gap-2">
                    <span className="text-sm text-muted-foreground">Read</span>
                    <Progress
                      value={Math.round(readPercent)}
                      className={cn('h-1 flex-1', hasNoProgressAndNoBookmarks && 'opacity-50')}
                    />
                    <span className="w-12 text-right text-sm text-muted-foreground">
                      {Math.round(readPercent)}%
                    </span>
                  </div>
                </Button>
              </CardContent>
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
            'flex overflow-hidden rounded-lg',
            isDisabled ? 'cursor-not-allowed' : 'cursor-pointer'
          )}
          onClick={isDisabled ? undefined : onSelect}
        >
          <div className="relative aspect-[3/4] h-20 p-2">{renderCover()}</div>
          <CardContent className="flex items-center justify-between w-full pl-2 pr-4">
            <div className="flex-1">
              <div className="space-y-1">
                <div className="flex items-center space-x-2">
                  <h3
                    className={cn(
                      'font-bold font-mono line-clamp-1',
                      hasNoProgressAndNoBookmarks && 'text-muted-foreground'
                    )}
                  >
                    {bookTitle}
                  </h3>
                  {source !== 'kobo-store' && (
                    <span className="shrink-0 px-1.5 py-0.5 rounded text-[9px] font-semibold uppercase tracking-wide bg-[#64748B] text-white">
                      {sourceLabel[source]}
                    </span>
                  )}
                  {contentType && (
                    <span className="shrink-0 px-1.5 py-0.5 rounded text-[9px] font-semibold uppercase tracking-wide bg-[#1E293B] text-white">
                      {contentType}
                    </span>
                  )}
                  <p title={author} className="text-sm text-muted-foreground">
                    {((): string => {
                      const authorsArray = (author || '').split(', ')
                      const firstThreeAuthors = authorsArray.slice(0, 3).join(', ')
                      const remainingAuthors =
                        authorsArray.slice(3).length > 0
                          ? `, ${authorsArray.slice(3).length} more`
                          : ''
                      return `${firstThreeAuthors}${remainingAuthors}`
                    })()}
                  </p>
                </div>
              </div>
            </div>
            <Button
              variant="ghost"
              className="flex items-center w-[35%] h-auto p-0 text-sm font-normal text-muted-foreground hover:text-foreground disabled:opacity-50"
              onClick={(e) => {
                e.stopPropagation()
                onPreview()
              }}
              disabled={!bookmarkCount || bookmarkCount <= 0}
            >
              <div className="w-20 text-right">{bookmarkCount ?? 0} notes</div>
              <Separator orientation="vertical" className="h-4 mx-2" />
              <div className="flex-1 flex items-center gap-2">
                <span className="text-sm text-muted-foreground">Read</span>
                <Progress
                  value={Math.round(readPercent)}
                  className={cn('h-1 flex-1', hasNoProgressAndNoBookmarks && 'opacity-50')}
                />
                <span className="w-12 text-right text-sm text-muted-foreground">
                  {Math.round(readPercent)}%
                </span>
              </div>
            </Button>
          </CardContent>
        </Card>
      )}
    </div>
  )
})

export function BookList({
  books,
  selectedBooks,
  onSelectBook,
  onPreviewBookmarks,
  isProcessing,
  currentBook,
  exportedBooks
}: BookListProps): React.JSX.Element {
  return (
    <TooltipProvider delayDuration={0}>
      <ScrollArea className="h-[calc(100vh-8rem)]">
        <div className="space-y-2 p-4 mb-12">
          {books.map((book) => (
            <BookListCard
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
