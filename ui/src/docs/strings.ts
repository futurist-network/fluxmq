/**
 * UI chrome of the docs page (search box, TOC, buttons). Kept next to the content instead of
 * the console dictionaries so the docs stay self-contained; same glossary as src/i18n.
 */
import type { DocGroup, DocLang } from './types';

const en = {
  title: 'Documentation',
  intro: 'How to run FluxMQ and use every screen of this console — task by task.',
  search: 'Search the docs…',
  searchLabel: 'Search the documentation',
  clear: 'Clear search',
  results: 'Results',
  noResults: 'Nothing found. Try another word or a shorter query.',
  articles: 'Articles',
  onThisPage: 'On this page',
  copy: 'Copy',
  copied: 'Copied',
  copyCode: 'Copy code',
  codeCopied: 'Code copied',
  copyLink: 'Copy link to this section',
  linkCopied: 'Link copied',
  copyFailed: 'Copy failed — select the text manually',
  previous: 'Previous',
  next: 'Next',
  notFound: 'Article not found',
  notFoundText: 'There is no article at this address. Pick one from the list or search.',
  backToDocs: 'All articles',
  loading: 'Loading documentation…',
  loadFailed: 'Could not load the documentation',
  retry: 'Retry',
  showArticles: 'Articles',
  hideArticles: 'Hide articles',
  readTime: '{n} min read',
  openInConsole: 'Open this screen in the console',
  backToConsole: 'Back to console',
  navLabel: 'Documentation sections',
  sections: 'Sections',
  closeSections: 'Close sections',
  breadcrumbs: 'Breadcrumbs',
  home: 'Docs',
  heroTitle: 'FluxMQ documentation',
  heroSubtitle: 'Set up the broker, wire up your services and find your way around every screen of this console.',
  heroSearch: 'Search: channel, consumer lag, DLQ, TLS…',
  popularTasks: 'Popular tasks',
  browseAll: 'Browse by section',
  searchShortcut: 'Press / to search',
  calloutNote: 'Note',
  calloutTip: 'Tip',
  calloutWarning: 'Warning',
  pagerLabel: 'Previous and next article',
  tasks: {
    'howto-create-channel': 'Create a channel',
    'howto-connect-client': 'Connect a client',
    recipes: 'Ready-made recipes',
    quickstart: 'Quick start',
    'howto-handle-dlq': 'Handle dead letters',
    'howto-investigate-lag': 'Investigate consumer lag',
    'howto-secure': 'Secure the broker',
  } as Record<string, string>,
};

export type DocStrings = typeof en;

const ru: DocStrings = {
  title: 'Документация',
  intro: 'Как запустить FluxMQ и пользоваться каждым экраном консоли — по задачам.',
  search: 'Поиск по документации…',
  searchLabel: 'Поиск по документации',
  clear: 'Очистить поиск',
  results: 'Результаты',
  noResults: 'Ничего не найдено. Попробуйте другое слово или запрос короче.',
  articles: 'Статьи',
  onThisPage: 'На этой странице',
  copy: 'Копировать',
  copied: 'Скопировано',
  copyCode: 'Скопировать код',
  codeCopied: 'Код скопирован',
  copyLink: 'Скопировать ссылку на раздел',
  linkCopied: 'Ссылка скопирована',
  copyFailed: 'Не удалось скопировать — выделите текст вручную',
  previous: 'Назад',
  next: 'Далее',
  notFound: 'Статья не найдена',
  notFoundText: 'По этому адресу статьи нет. Выберите её в списке или воспользуйтесь поиском.',
  backToDocs: 'Все статьи',
  loading: 'Загружаем документацию…',
  loadFailed: 'Не удалось загрузить документацию',
  retry: 'Повторить',
  showArticles: 'Статьи',
  hideArticles: 'Скрыть список',
  readTime: '{n} мин чтения',
  openInConsole: 'Открыть этот экран в консоли',
  backToConsole: 'Вернуться в консоль',
  navLabel: 'Разделы документации',
  sections: 'Разделы',
  closeSections: 'Закрыть разделы',
  breadcrumbs: 'Навигационная цепочка',
  home: 'Документация',
  heroTitle: 'Документация FluxMQ',
  heroSubtitle: 'Как поднять брокер, подключить сервисы и разобраться в каждом экране консоли.',
  heroSearch: 'Поиск: канал, отставание, DLQ, TLS…',
  popularTasks: 'Частые задачи',
  browseAll: 'Все разделы',
  searchShortcut: 'Нажмите / для поиска',
  calloutNote: 'Примечание',
  calloutTip: 'Совет',
  calloutWarning: 'Внимание',
  pagerLabel: 'Предыдущая и следующая статья',
  tasks: {
    'howto-create-channel': 'Создать канал',
    'howto-connect-client': 'Подключить клиента',
    recipes: 'Готовые рецепты',
    quickstart: 'Быстрый старт',
    'howto-handle-dlq': 'Разобрать мёртвые письма (DLQ)',
    'howto-investigate-lag': 'Найти причину отставания',
    'howto-secure': 'Защитить брокер',
  },
};

export const DOC_STRINGS: Record<DocLang, DocStrings> = { en, ru };

export const GROUP_LABEL: Record<DocLang, Record<DocGroup, string>> = {
  ru: { start: 'Начало работы', howto: 'Как сделать', console: 'Справочник по экранам консоли', reference: 'Технический справочник' },
  en: { start: 'Getting started', howto: 'How-to guides', console: 'Console screen reference', reference: 'Technical reference' },
};

/** One line under each group heading on the docs home page. */
export const GROUP_BLURB: Record<DocLang, Record<DocGroup, string>> = {
  ru: {
    start: 'Что такое FluxMQ, основные понятия и первый запуск.',
    howto: 'Пошаговые инструкции под конкретную задачу.',
    console: 'Что показывает каждый экран консоли и что на нём можно сделать.',
    reference: 'REST API, CLI, метрики, коды ошибок и глоссарий.',
  },
  en: {
    start: 'What FluxMQ is, the core concepts and a first run.',
    howto: 'Step-by-step instructions for a specific task.',
    console: 'What every console screen shows and what you can do there.',
    reference: 'REST API, CLI, metrics, error codes and the glossary.',
  },
};

/**
 * "Popular tasks" on the docs home page, in display order. The first slug of each entry that
 * exists in the loaded set is used, so a missing article (e.g. recipes not written yet) falls
 * back instead of leaving a dead card.
 */
export const POPULAR_TASKS: readonly (readonly string[])[] = [
  ['howto-create-channel'],
  ['howto-connect-client'],
  ['recipes', 'quickstart'],
  ['howto-handle-dlq'],
  ['howto-investigate-lag'],
  ['howto-secure'],
];
