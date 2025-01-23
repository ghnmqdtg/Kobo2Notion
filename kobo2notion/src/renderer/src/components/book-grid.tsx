import { Book } from '../../../backend/models';
import { Card, CardContent, CardFooter } from '@/components/ui/card';
import { Checkbox } from '@/components/ui/checkbox';

interface BookGridProps {
    books: Book[];
    selectedBooks: Set<string>;
    onSelectBook: (bookTitle: string) => void;
}

export function BookGrid({ books, selectedBooks, onSelectBook }: BookGridProps) {
    return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 p-4">
            {books.map((book) => (
                <Card key={book.bookTitle} className="flex flex-col">
                    <CardContent className="flex-grow p-4">
                        <div className="flex items-start space-x-4">
                            <Checkbox
                                checked={selectedBooks.has(book.bookTitle)}
                                onCheckedChange={() => onSelectBook(book.bookTitle)}
                            />
                            <div>
                                <h3 className="font-bold">{book.bookTitle}</h3>
                                {book.subtitle && <p className="text-sm text-gray-500">{book.subtitle}</p>}
                                <p className="text-sm text-gray-500">{book.author}</p>
                            </div>
                        </div>
                    </CardContent>
                    <CardFooter className="p-4 pt-0">
                        <div className="text-sm text-gray-500">
                            Read: {Math.round(book.readPercent * 100)}%
                        </div>
                    </CardFooter>
                </Card>
            ))}
        </div>
    );
} 