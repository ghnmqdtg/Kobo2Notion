// This file serves as a central location to define the data structures (or data models) that our application works with.

// Kobo Models
export interface Book {
    bookTitle: string;
    subtitle: string | null;
    author: string;
    publisher: string;
    isbn: string;
    series: string | null;
    seriesNumber: number | null;
    readPercent: number;
    imageId: string | null;
}

export interface Bookmark {
    volumeId: string;
    highlight: string;
    annotation: string | null;
    createdOn: string;
    type: string;
}