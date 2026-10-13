import type { RuOf } from '../types';
import type { errors as en } from '../en/errors';

export const errors: RuOf<typeof en> = {
  'errors.network': 'Сервер FluxMQ недоступен',
  'errors.unauthorized': 'Требуется аутентификация — токен доступа не задан, отозван или просрочен.',
  'errors.forbidden': 'Ваша роль не позволяет выполнить это действие.',
  'errors.not_found': 'Не найдено.',
  'errors.unavailable': 'Брокер временно недоступен. Повторите попытку чуть позже.',
  'errors.internal': 'Внутренняя ошибка сервера. Проверьте журналы брокера.',
  'errors.unsupported_in_cluster': 'Недоступно на узле кластера.',
  'errors.unsupported_single_node': 'Доступно только в кластере — этот брокер работает как одиночный узел.',
  'errors.last_admin': 'Это последний активный токен администратора. Прежде чем отзывать его, создайте ещё один токен администратора (или запустите брокер с --admin-token).',
  'errors.withDetail': '{message} ({detail})',
  'errors.http': 'HTTP {status}',
};
