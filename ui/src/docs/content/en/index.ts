/** Every `*.md` in this folder, as raw text keyed by slug (bundled into one lazy chunk). */
import { rawBySlug } from '../../registry';

const modules = import.meta.glob<string>('./*.md', { query: '?raw', import: 'default', eager: true });

export default rawBySlug(modules);
