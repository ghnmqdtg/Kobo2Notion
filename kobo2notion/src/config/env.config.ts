import dotenv from 'dotenv';
import path from 'path';

// Load environment variables from .env file
// ! The path should be update after electron migration
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

interface EnvironmentConfig {
    SQLITE_SOURCE: string;
    NOTION_API_KEY: string;
    NOTION_DATABASE_ID: string;
    GEMINI_API_KEY: string;
    GEMINI_MODEL: string;
    SUMMARIZE_ENABLED: boolean;
    SUMMARIZE_LANGUAGE: string;
}

// Validate environment variables
const getConfig = (): EnvironmentConfig => {

    const config = {
        SQLITE_SOURCE: process.env.SQLITE_SOURCE,
        NOTION_API_KEY: process.env.NOTION_API,
        NOTION_DATABASE_ID: process.env.NOTION_DB,
        GEMINI_API_KEY: process.env.GEMINI_API,
        GEMINI_MODEL: process.env.GEMINI_MODEL,
        SUMMARIZE_ENABLED: process.env.SUMMARIZE_ENABLED === 'true',
        SUMMARIZE_LANGUAGE: process.env.SUMMARIZE_LANGUAGE,
    };

    console.info('config', config);

    // Validate that all required environment variables are present
    const missingKeys = Object.entries(config)
        .filter(([_, value]) => !value)
        .map(([key]) => key);

    if (missingKeys.length > 0) {
        throw new Error(
            `Missing required environment variables: ${missingKeys.join(', ')}`
        );
    }

    return config as EnvironmentConfig;
};

export const env = getConfig(); 