import { CheckSquare, LayoutGrid, List } from "lucide-react";
import { Toggle } from "@/components/ui/toggle";
import { BookSource } from "../../../../backend/models";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";

interface HeaderProps {
    selectAll: boolean;
    setSelectAll: (selected: boolean) => void;
    isGridView: boolean;
    setIsGridView: (isGrid: boolean) => void;
    isDisabled: boolean;
    sourceFilter: Set<BookSource>;
    setSourceFilter: (filter: Set<BookSource>) => void;
}

const sources: { value: BookSource; label: string }[] = [
    { value: "kobo-store", label: "Kobo" },
    { value: "external", label: "External" },
];

export function Header({
    selectAll,
    setSelectAll,
    isGridView,
    setIsGridView,
    isDisabled,
    sourceFilter,
    setSourceFilter,
}: HeaderProps) {
    return (
        <div className="flex justify-between items-center p-4 pb-0">
            <h1 className="text-2xl font-bold">Your Books</h1>

            <div className="flex items-center gap-2">
                <ToggleGroup
                    type="multiple"
                    value={Array.from(sourceFilter)}
                    onValueChange={(value: string[]) => {
                        if (value.length === 0) return;
                        setSourceFilter(new Set(value as BookSource[]));
                    }}
                    variant="outline"
                    size="sm"
                    disabled={isDisabled}
                >
                    {sources.map(({ value, label }) => (
                        <ToggleGroupItem key={value} value={value}>
                            {label}
                        </ToggleGroupItem>
                    ))}
                </ToggleGroup>

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
