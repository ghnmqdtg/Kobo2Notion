import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}

export function formatAuthors(author: string): string {
  const authorsArray = (author || "").split(", ");
  const firstThree = authorsArray.slice(0, 3).join(", ");
  const remaining =
    authorsArray.length > 3 ? `, ${authorsArray.length - 3} more` : "";
  return `${firstThree}${remaining}`;
}
