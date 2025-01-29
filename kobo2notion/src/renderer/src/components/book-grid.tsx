import { useState, useEffect } from 'react';
import { Book } from '../../../backend/models';
import { Card, CardContent, CardFooter } from '@/components/ui/card';
import { Progress } from "@/components/ui/progress";
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
    const [progress, setProgress] = useState<number>(Math.round(readPercent));

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
        <div className={cn(
            "relative rounded-lg h-full",
            "before:absolute before:inset-0 before:rounded-lg before:transition-all",
            "before:pointer-events-none",
            isSelected
                ? "before:border-2 before:border-primary before:-m-[2px]"
                : "before:border before:border-border hover:before:border-primary"
        )}>
            <Card
                className="flex flex-col overflow-hidden cursor-pointer rounded-lg h-full"
                onClick={onSelect}
            >
                <div className="relative aspect-[3/4] w-full overflow-hidden p-4 shrink-0">
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
                <CardContent className="flex-1 p-4">
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
                <CardFooter className="p-4 pt-0 mt-auto shrink-0">
                    <div className="w-full flex items-center gap-2">
                        <span className="text-sm text-muted-foreground">
                            Read
                        </span>
                        <Progress
                            value={progress}
                            className="h-1"
                        />
                        <span className="text-sm text-muted-foreground">
                            {Math.round(progress)}%
                        </span>
                    </div>
                </CardFooter>
            </Card>
        </div>
    );
}

export function BookGrid({ books, selectedBooks, onSelectBook }: BookGridProps) {
    return (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-6 p-4 auto-rows-fr">
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