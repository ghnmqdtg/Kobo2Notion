import { useState, useEffect } from 'react';
import { Book } from '../../../backend/models';
import { Card, CardContent, CardFooter } from '@/components/ui/card';
import { Checkbox } from '@/components/ui/checkbox';
import { cn } from '@/lib/utils';

interface BookGridProps {
    books: Book[];
    selectedBooks: Set<string>;
    onSelectBook: (bookTitle: string) => void;
}

interface BookCardProps extends Book {
    isSelected: boolean;
    onSelect: () => void;
}

function BookCard({ bookTitle, subtitle, author, readPercent, isbn, isSelected, onSelect }: BookCardProps) {
    const [coverUrl, setCoverUrl] = useState<string>('');

    useEffect(() => {
        const loadCover = async () => {
            try {
                const url = await window.api.fetchBookCover(bookTitle, isbn);
                setCoverUrl(url);
            } catch (error) {
                console.error('Error loading book cover:', error);
            }
        };
        loadCover();
    }, [bookTitle, isbn]);

    return (
        <Card
            className={cn(
                "flex flex-col overflow-hidden cursor-pointer transition-all border border-border hover:border-primary",
                isSelected && "border-2 border-primary"
            )}
            onClick={onSelect}
        >
            <div className="relative aspect-[3/4] w-full overflow-hidden">
                {coverUrl ? (
                    <img
                        src={coverUrl}
                        alt={`${bookTitle} cover`}
                        className="object-cover w-full h-full select-none"
                        draggable="false"
                    />
                ) : (
                    <div className="w-full h-full bg-muted flex items-center justify-center">
                        <span className="text-muted-foreground">No cover</span>
                    </div>
                )}
            </div>
            <CardContent className="flex-grow p-4">
                <div className="space-y-1">
                    <h3 className="font-bold line-clamp-2">{bookTitle}</h3>
                    {subtitle && (
                        <p className="text-sm text-muted-foreground line-clamp-2">
                            {subtitle}
                        </p>
                    )}
                    <p className="text-sm text-muted-foreground">{author}</p>
                </div>
            </CardContent>
            <CardFooter className="p-4 pt-0">
                <div className="text-sm text-muted-foreground">
                    Read: {Math.round(readPercent)}%
                </div>
            </CardFooter>
        </Card>
    );
}

export function BookGrid({ books, selectedBooks, onSelectBook }: BookGridProps) {
    return (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4 p-4">
            {books.map((book) => (
                <BookCard
                    key={book.bookTitle}
                    {...book}
                    isSelected={selectedBooks.has(book.bookTitle)}
                    onSelect={() => onSelectBook(book.bookTitle)}
                />
            ))}
        </div>
    );
} 