import { CheckSquare, Filter, LayoutGrid, List } from "lucide-react";
import { Toggle } from "@/components/ui/toggle";
import { Button } from "@/components/ui/button";
import { BookSource } from "../../../../backend/models";
import {
    DropdownMenu,
    DropdownMenuCheckboxItem,
    DropdownMenuContent,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

interface HeaderProps {
    selectAll: boolean;
    setSelectAll: (selected: boolean) => void;
    isGridView: boolean;
    setIsGridView: (isGrid: boolean) => void;
    isDisabled: boolean;
    sourceFilter: Set<BookSource>;
    onSourceFilterChange: (source: BookSource) => void;
}

const sources: { value: BookSource; label: string }[] = [
    { value: "kobo-store", label: "Kobo" },
    { value: "instapaper", label: "Instapaper" },
    { value: "external", label: "External" },
];

export function Header({
    selectAll,
    setSelectAll,
    isGridView,
    setIsGridView,
    isDisabled,
    sourceFilter,
    onSourceFilterChange,
}: HeaderProps) {
    const allSelected = sourceFilter.size === sources.length;

    return (
        <div className="flex justify-between items-center p-4 pb-0">
            <h1 className="text-2xl font-bold">Your Books</h1>

            <div className="flex items-center gap-2">
                <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                        <Button variant="outline" size="sm" className="gap-1.5" disabled={isDisabled}>
                            <Filter className="h-4 w-4" />
                            <span>Source{!allSelected && ` (${sourceFilter.size})`}</span>
                        </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                        <DropdownMenuLabel>Filter by source</DropdownMenuLabel>
                        <DropdownMenuSeparator />
                        {sources.map(({ value, label }) => (
                            <DropdownMenuCheckboxItem
                                key={value}
                                checked={sourceFilter.has(value)}
                                onCheckedChange={() => onSourceFilterChange(value)}
                                onSelect={(e) => e.preventDefault()}
                            >
                                {label}
                            </DropdownMenuCheckboxItem>
                        ))}
                    </DropdownMenuContent>
                </DropdownMenu>

                <Toggle
                    pressed={selectAll}
                    onPressedChange={setSelectAll}
                    aria-label="Toggle select all"
                    disabled={isDisabled}
                >
                    <CheckSquare className="h-4 w-4" />
                    <span>Select all</span>
                </Toggle>

                <Toggle
                    pressed={isGridView}
                    onPressedChange={setIsGridView}
                    aria-label="Toggle view"
                    className="w-[110px]"
                >
                    {isGridView ? (
                        <>
                            <LayoutGrid className="h-4 w-4" />
                            <span>Grid view</span>
                        </>
                    ) : (
                        <>
                            <List className="h-4 w-4" />
                            <span>List view</span>
                        </>
                    )}
                </Toggle>
            </div>
        </div>
    );
}
