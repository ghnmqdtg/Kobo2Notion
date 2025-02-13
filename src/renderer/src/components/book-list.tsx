import { Book } from '../../../backend/models';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Progress } from "@/components/ui/progress";
import { cn } from '@/lib/utils';

interface BookListProps {
    books: Book[];
    selectedBooks: Set<string>;
    onSelectBook: (bookTitle: string) => void;
}

export function BookList({ books, selectedBooks, onSelectBook }: BookListProps) {
    return (
        <ScrollArea className="h-[calc(100vh-8rem)]">
            <div className="space-y-2 p-4">
                {books.map((book) => (
                    <div
                        key={book.bookTitle}
                        className={cn(
                            "flex items-center space-x-4 p-4 rounded-lg cursor-pointer",
                            "border transition-all",
                            selectedBooks.has(book.bookTitle)
                                ? "border-2 border-primary -m-[1px]"
                                : "border-border hover:border-primary"
                        )}
                        onClick={() => onSelectBook(book.bookTitle)}
                    >
                        <div className="flex-1">
                            <h3 className="font-bold">{book.bookTitle}</h3>
                            <p className="text-sm text-muted-foreground">{book.author}</p>
                        </div>
                        <div className="flex items-center space-x-4 w-1/4">
                            <span className="text-sm text-muted-foreground">
                                Read
                            </span>
                            <Progress
                                value={Math.round(book.readPercent)}
                                className="h-2"
                            />
                            <span className="text-sm text-muted-foreground w-12">
                                {Math.round(book.readPercent)}%
                            </span>
                        </div>
                    </div>
                ))}
            </div>
        </ScrollArea>
    );
} 