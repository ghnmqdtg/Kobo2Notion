import { Settings } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ThemeToggle } from './theme-toggle';
import { Logo } from './logo';

interface NavbarProps {
    onSettingsClick: () => void;
    isFirstTime?: boolean;
}

export function Navbar({ onSettingsClick, isFirstTime }: NavbarProps) {
    return (
        <nav className="border-b">
            <div className="flex h-16 items-center px-4">
                <Logo />
                <div className="ml-auto flex items-center space-x-2">
                    <ThemeToggle />
                    <Button
                        variant="ghost"
                        size="icon"
                        onClick={onSettingsClick}
                        disabled={isFirstTime}
                        title={isFirstTime ? "Please complete the initial setup first" : "Settings"}
                    >
                        <Settings className="h-5 w-5" />
                    </Button>
                </div>
            </div>
        </nav>
    );
}