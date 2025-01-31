import { Settings } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from '@/components/ui/sheet';
import { Input } from '@/components/ui/input';
import { Separator } from '@/components/ui/separator';

export function Navbar() {
    return (
        <nav className="border-b">
            <div className="flex h-16 items-center px-4">
                <div className="flex items-center space-x-4">
                    <h2 className="text-3xl font-bold">Kobo2Notion</h2>
                </div>
                <div className="ml-auto flex items-center space-x-4">
                    <Sheet>
                        <SheetTrigger asChild>
                            <Button variant="ghost" size="icon">
                                <Settings className="h-5 w-5" />
                            </Button>
                        </SheetTrigger>
                        <SheetContent>
                            <SheetHeader>
                                <SheetTitle>Settings</SheetTitle>
                            </SheetHeader>
                            <div className="grid gap-4 py-4">
                                <div className="space-y-2">
                                    <label className="text-sm font-medium">Kobo Highlights File Path</label>
                                    <Input type="text" value={window.env.SQLITE_SOURCE} readOnly />
                                </div>
                                <Separator />
                                <div className="space-y-2">
                                    <label className="text-sm font-medium">Notion API Key</label>
                                    <Input type="password" value={window.env.NOTION_API_KEY} readOnly />
                                </div>
                                <div className="space-y-2">
                                    <label className="text-sm font-medium">Notion Database ID</label>
                                    <Input type="password" value={window.env.NOTION_DATABASE_ID} readOnly />
                                </div>
                                <Separator />
                                <div className="space-y-2">
                                    <label className="text-sm font-medium">Gemini API Key</label>
                                    <Input type="password" value={window.env.GEMINI_API_KEY} readOnly />
                                </div>
                            </div>
                        </SheetContent>
                    </Sheet>
                </div>
            </div>
        </nav>
    );
} 