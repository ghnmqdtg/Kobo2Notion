import { Settings } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ThemeToggle } from './theme-toggle';

interface NavbarProps {
    onSettingsClick: () => void;
}

export function Navbar({ onSettingsClick }: NavbarProps) {
    return (
        <nav className="border-b">
            <div className="flex h-16 items-center px-4">
                <div className="flex items-center space-x-4">
                    <h2 className="text-3xl font-bold">Kobo2Notion</h2>
                </div>
                <div className="ml-auto flex items-center space-x-2">
                    <ThemeToggle />
                    <Button variant="ghost" size="icon" onClick={onSettingsClick}>
                        <Settings className="h-5 w-5" />
                    </Button>
                </div>
            </div>
        </nav>
    );
} 