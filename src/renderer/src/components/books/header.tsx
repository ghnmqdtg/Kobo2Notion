import { CheckSquare, LayoutGrid, List } from "lucide-react";
import { Toggle } from "@/components/ui/toggle";

interface HeaderProps {
  selectAll: boolean;
  setSelectAll: (selected: boolean) => void;
  isGridView: boolean;
  setIsGridView: (isGrid: boolean) => void;
  isDisabled: boolean;
}

export function Header({
  selectAll,
  setSelectAll,
  isGridView,
  setIsGridView,
  isDisabled,
}: HeaderProps): React.JSX.Element {
  return (
    <div className="flex justify-between items-center p-4 pb-0">
      <h1 className="text-2xl font-bold">Your Books</h1>

      <div className="flex items-center gap-2">
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
