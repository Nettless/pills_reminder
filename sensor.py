import logging
from datetime import datetime, timedelta
from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.helpers.storage import Store
from homeassistant.helpers import device_registry as dr, entity_registry as er
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Pills Reminder sensors."""
    coordinator = PillsDataCoordinator(hass, config_entry)
    
    # Сохраняем координатор в данные интеграции
    if config_entry.entry_id in hass.data[DOMAIN]:
        hass.data[DOMAIN][config_entry.entry_id]['coordinator'] = coordinator
    
    coordinator.async_add_entities_callback = async_add_entities
    
    await coordinator.async_config_entry_first_refresh()
    
    sensors = []
    
    # Создаем общий сенсор статистики
    sensors.append(PillsStatisticsSensor(coordinator, config_entry))
    
    # Создаем сенсоры для каждого пользователя и его лекарств
    for user_id, user_data in coordinator.data.get('users', {}).items():
        # Общие сенсоры пользователя
        sensors.extend([
            UserStatisticsSensor(coordinator, config_entry, user_id),
            UserComplianceSensor(coordinator, config_entry, user_id),
        ])
        
        # Сенсоры для каждого лекарства пользователя
        for pill_name in user_data.get('pills', {}):
            sensors.extend([
                UserPillSensor(coordinator, config_entry, user_id, pill_name, 'taken_today'),
                UserPillSensor(coordinator, config_entry, user_id, pill_name, 'taken_week'),
                UserPillSensor(coordinator, config_entry, user_id, pill_name, 'skipped_today'),
                UserPillSensor(coordinator, config_entry, user_id, pill_name, 'skipped_week'),
                UserPillSensor(coordinator, config_entry, user_id, pill_name, 'compliance_week'),
                UserPillSensor(coordinator, config_entry, user_id, pill_name, 'last_taken'),
                UserPillSensor(coordinator, config_entry, user_id, pill_name, 'next_due'),
                UserPillSensor(coordinator, config_entry, user_id, pill_name, 'course_progress'),
            ])
    
    async_add_entities(sensors)
    _LOGGER.info(f"Created {len(sensors)} sensors for pills reminder")

class PillsDataCoordinator(DataUpdateCoordinator):
    """Data coordinator for pills reminder."""
    
    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry):
        super().__init__(
            hass,
            _LOGGER,
            name="Pills Reminder",
            update_interval=timedelta(minutes=1),
        )
        self.config_entry = config_entry
        self.storage = Store(hass, 1, f"pills_reminder_global")
        self.users_storage = Store(hass, 1, f"pills_reminder_users")
        self.async_add_entities_callback = None
        self._known_user_pills = {}  # {user_id: set(pill_names)}
        self._known_users = set()  # Отслеживаем известных пользователей

    async def _async_update_data(self):
        """Fetch data from storage."""
        try:
            history_data = await self.storage.async_load() or {'history': []}
            users_data = await self.users_storage.async_load() or {}
            
            # Обрабатываем данные по пользователям
            users_pills_data = {}
            all_stats = {'total_users': 0, 'total_pills': 0, 'total_taken_today': 0, 'total_skipped_today': 0}
            
            # Проверяем на новых пользователей
            current_users = set(users_data.keys())
            new_users = current_users - self._known_users
            deleted_users = self._known_users - current_users
            
            # Удаляем устройства для удаленных пользователей
            if deleted_users:
                await self._cleanup_deleted_users(deleted_users)
            
            self._known_users = current_users.copy()
            
            for user_id, user_data in users_data.items():
                if not user_data.get('reminders'):
                    continue
                    
                username = user_data.get('username', user_data.get('first_name', f'User_{user_id}'))
                
                # Собираем все лекарства пользователя
                user_pills = set()
                for reminder_id, reminder in user_data.get('reminders', {}).items():
                    pill_name = reminder.get('pill_name')
                    if pill_name and reminder.get('active', True):
                        user_pills.add(pill_name)
                
                # Инициализируем известные лекарства для нового пользователя
                if user_id not in self._known_user_pills:
                    self._known_user_pills[user_id] = set()
                
                # Проверяем на удаленные лекарства
                deleted_pills = self._known_user_pills[user_id] - user_pills
                if deleted_pills:
                    await self._cleanup_deleted_pills(user_id, deleted_pills)
                
                # Проверяем на новые лекарства для этого пользователя
                new_pills = user_pills - self._known_user_pills[user_id]
                
                # Создаем сенсоры для нового пользователя или новых лекарств
                if user_id in new_users and self.async_add_entities_callback:
                    # Новый пользователь - создаем все сенсоры
                    await self._create_sensors_for_new_user(user_id, user_pills)
                    self._known_user_pills[user_id].update(user_pills)
                elif new_pills and self.async_add_entities_callback:
                    # Новые лекарства для существующего пользователя
                    await self._create_sensors_for_new_user_pills(user_id, new_pills)
                    self._known_user_pills[user_id].update(new_pills)
                
                self._known_user_pills[user_id] = user_pills.copy()
                
                # Обрабатываем данные пользователя
                user_pills_data = {}
                user_stats = {'taken_today': 0, 'skipped_today': 0, 'taken_week': 0, 'skipped_week': 0}
                
                for pill_name in user_pills:
                    pill_data = await self._process_user_pill_data(user_id, pill_name, history_data, user_data)
                    user_pills_data[pill_name] = pill_data
                    
                    # Добавляем к общей статистике пользователя
                    user_stats['taken_today'] += pill_data['taken_today']
                    user_stats['skipped_today'] += pill_data['skipped_today']
                    user_stats['taken_week'] += pill_data['taken_week']
                    user_stats['skipped_week'] += pill_data['skipped_week']
                
                # Рассчитываем соблюдение для пользователя
                total_week = user_stats['taken_week'] + user_stats['skipped_week']
                user_stats['compliance_week'] = round((user_stats['taken_week'] / total_week * 100) if total_week > 0 else 100, 1)
                user_stats['active_reminders'] = len([r for r in user_data.get('reminders', {}).values() if r.get('active', True)])
                
                users_pills_data[user_id] = {
                    'username': username,
                    'pills': user_pills_data,
                    'stats': user_stats,
                    'reminders': user_data.get('reminders', {})
                }
                
                # Добавляем к общей статистике
                all_stats['total_taken_today'] += user_stats['taken_today']
                all_stats['total_skipped_today'] += user_stats['skipped_today']
                all_stats['total_pills'] += len(user_pills)
            
            all_stats['total_users'] = len(users_pills_data)
            
            return {
                'users': users_pills_data,
                'total': all_stats,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as err:
            raise UpdateFailed(f"Error fetching pills data: {err}")

    async def _cleanup_deleted_users(self, deleted_user_ids):
        """Удаляет устройства для удаленных пользователей"""
        try:
            device_registry = dr.async_get(self.hass)
            entity_registry = er.async_get(self.hass)
            
            for user_id in deleted_user_ids:
                # Удаляем устройство пользователя
                user_device_id = (DOMAIN, f"{self.config_entry.entry_id}_user_{user_id}")
                device = device_registry.async_get_device(identifiers={user_device_id})
                if device:
                    device_registry.async_remove_device(device.id)
                    _LOGGER.info(f"Removed user device for user {user_id}")
                
                # Удаляем устройства лекарств пользователя (если есть известные)
                if user_id in self._known_user_pills:
                    for pill_name in self._known_user_pills[user_id]:
                        await self._cleanup_pill_device(user_id, pill_name)
                
                # Очищаем из известных пользователей
                if user_id in self._known_user_pills:
                    del self._known_user_pills[user_id]
                    
        except Exception as err:
            _LOGGER.error(f"Error cleaning up deleted users: {err}")

    async def _cleanup_deleted_pills(self, user_id, deleted_pills):
        """Удаляет устройства для удаленных лекарств"""
        try:
            for pill_name in deleted_pills:
                await self._cleanup_pill_device(user_id, pill_name)
        except Exception as err:
            _LOGGER.error(f"Error cleaning up deleted pills: {err}")

    async def _cleanup_pill_device(self, user_id, pill_name):
        """Удаляет устройство конкретного лекарства"""
        try:
            device_registry = dr.async_get(self.hass)
            safe_pill_name = pill_name.replace(' ', '_').replace('-', '_').lower()
            pill_device_id = (DOMAIN, f"{self.config_entry.entry_id}_user_{user_id}_{safe_pill_name}")
            device = device_registry.async_get_device(identifiers={pill_device_id})
            if device:
                device_registry.async_remove_device(device.id)
                _LOGGER.info(f"Removed pill device for user {user_id}, pill {pill_name}")
        except Exception as err:
            _LOGGER.error(f"Error cleaning up pill device: {err}")

    async def _create_sensors_for_new_user(self, user_id, user_pills):
        """Create all sensors for new user."""
        if not self.async_add_entities_callback:
            return
            
        # Сначала загружаем данные пользователя для получения правильного username
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(user_id, {})
        
        new_sensors = []
        
        # Общие сенсоры пользователя
        new_sensors.extend([
            UserStatisticsSensor(self, self.config_entry, user_id),
            UserComplianceSensor(self, self.config_entry, user_id),
        ])
        
        # Сенсоры для каждого лекарства
        for pill_name in user_pills:
            new_sensors.extend([
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'taken_today'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'taken_week'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'skipped_today'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'skipped_week'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'compliance_week'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'last_taken'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'next_due'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'course_progress'),
            ])
        
        if new_sensors:
            self.async_add_entities_callback(new_sensors)
            username = user_data.get('username', f'User_{user_id}')
            _LOGGER.info(f"Created {len(new_sensors)} sensors for new user {username}")

    async def _create_sensors_for_new_user_pills(self, user_id, new_pills):
        """Create sensors for new user pills."""
        if not self.async_add_entities_callback:
            return
            
        new_sensors = []
        
        for pill_name in new_pills:
            new_sensors.extend([
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'taken_today'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'taken_week'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'skipped_today'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'skipped_week'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'compliance_week'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'last_taken'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'next_due'),
                UserPillSensor(self, self.config_entry, user_id, pill_name, 'course_progress'),
            ])
        
        if new_sensors:
            self.async_add_entities_callback(new_sensors)
            users_data = await self.users_storage.async_load() or {}
            username = users_data.get(user_id, {}).get('username', f'User_{user_id}')
            _LOGGER.info(f"Created sensors for new pills for {username}: {', '.join(new_pills)}")

    async def _process_user_pill_data(self, user_id, pill_name, history_data, user_data):
        """Process data for specific user's pill."""
        now = datetime.now()
        today = now.date()
        week_ago = now - timedelta(days=7)
        
        # Фильтруем историю для конкретного пользователя и лекарства
        user_pill_history = [
            entry for entry in history_data.get('history', [])
            if (entry.get('user_id') == str(user_id) and
                entry.get('pill_name') == pill_name)
        ]
        
        # Сегодняшняя статистика
        today_entries = [
            entry for entry in user_pill_history
            if datetime.fromisoformat(entry['date']).date() == today
        ]
        taken_today = sum(1 for entry in today_entries if entry['status'] == 'taken')
        skipped_today = sum(1 for entry in today_entries if entry['status'] == 'skipped')
        
        # Недельная статистика
        week_entries = [
            entry for entry in user_pill_history
            if datetime.fromisoformat(entry['date']) >= week_ago
        ]
        taken_week = sum(1 for entry in week_entries if entry['status'] == 'taken')
        skipped_week = sum(1 for entry in week_entries if entry['status'] == 'skipped')
        
        # Процент соблюдения за неделю
        total_week = taken_week + skipped_week
        compliance_week = round((taken_week / total_week * 100) if total_week > 0 else 100, 1)
        
        # Последний прием
        taken_entries = [
            entry for entry in user_pill_history
            if entry['status'] == 'taken'
        ]
        last_taken = None
        if taken_entries:
            last_taken = max(taken_entries, key=lambda x: x['date'])['date']
        
        # Следующий прием
        next_due = self._calculate_next_due(user_data, pill_name)
        
        # Прогресс курса
        course_progress = self._calculate_course_progress(user_data, pill_name)
        
        return {
            'taken_today': taken_today,
            'taken_week': taken_week,
            'skipped_today': skipped_today,
            'skipped_week': skipped_week,
            'compliance_week': compliance_week,
            'last_taken': last_taken,
            'next_due': next_due,
            'course_progress': course_progress,
            'total_today': taken_today + skipped_today,
            'total_week': total_week,
        }

    def _calculate_next_due(self, user_data, pill_name):
        """Calculate next due time for pill."""
        now = datetime.now()
        
        # Ищем активные напоминания для этого лекарства
        for reminder_id, reminder in user_data.get('reminders', {}).items():
            if (reminder.get('pill_name') == pill_name and
                reminder.get('active', True)):
                
                times = reminder.get('times', [])
                if times:
                    # Найдем ближайшее время приема
                    current_time = now.time()
                    today_times = []
                    tomorrow_times = []
                    
                    for time_slot in times:
                        try:
                            time_obj = datetime.strptime(time_slot.get('time', ''), "%H:%M").time()
                            next_due = now.replace(
                                hour=time_obj.hour,
                                minute=time_obj.minute,
                                second=0,
                                microsecond=0
                            )
                            
                            if next_due.time() > current_time:
                                today_times.append(next_due)
                            else:
                                tomorrow_times.append(next_due + timedelta(days=1))
                        except ValueError:
                            continue
                    
                    # Возвращаем ближайшее время
                    if today_times:
                        return min(today_times).isoformat()
                    elif tomorrow_times:
                        return min(tomorrow_times).isoformat()
        
        return None

    def _calculate_course_progress(self, user_data, pill_name):
        """Calculate course progress for pill."""
        for reminder_id, reminder in user_data.get('reminders', {}).items():
            if (reminder.get('pill_name') == pill_name and
                reminder.get('active', True)):
                
                duration_days = reminder.get('duration_days')
                if duration_days:
                    try:
                        start_date = datetime.fromisoformat(reminder.get('created', datetime.now().isoformat()))
                        days_passed = (datetime.now() - start_date).days + 1
                        days_left = max(0, duration_days - days_passed + 1)
                        
                        return {
                            'days_passed': days_passed,
                            'total_days': duration_days,
                            'days_left': days_left,
                            'progress_percent': round((days_passed / duration_days * 100), 1) if duration_days > 0 else 0
                        }
                    except Exception:
                        pass
                
                return {
                    'days_passed': 0,
                    'total_days': None,
                    'days_left': None,
                    'progress_percent': None
                }
        
        return {
            'days_passed': 0,
            'total_days': None,
            'days_left': None,
            'progress_percent': None
        }

class PillsStatisticsSensor(SensorEntity):
    """General statistics sensor."""
    
    def __init__(self, coordinator: PillsDataCoordinator, config_entry: ConfigEntry):
        self.coordinator = coordinator
        self.config_entry = config_entry
        self._attr_unique_id = f"{config_entry.entry_id}_pills_total_statistics"
        self._attr_name = "Pills Total Statistics"
        self._attr_icon = "mdi:pill"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self.config_entry.entry_id)},
            "name": "Pills Reminder System",
            "manufacturer": "Pills Reminder Bot",
            "model": "Statistics Hub",
            "sw_version": "1.0",
        }

    @property
    def state(self):
        total_data = self.coordinator.data.get('total', {})
        return total_data.get('total_users', 0)

    @property
    def extra_state_attributes(self):
        total_data = self.coordinator.data.get('total', {})
        return {
            'total_users': total_data.get('total_users', 0),
            'total_pills': total_data.get('total_pills', 0),
            'total_taken_today': total_data.get('total_taken_today', 0),
            'total_skipped_today': total_data.get('total_skipped_today', 0),
            'last_updated': self.coordinator.data.get('last_updated'),
            'friendly_name': 'Общая статистика системы напоминаний',
            'unit_of_measurement': 'users'
        }

    @callback
    def _handle_coordinator_update(self):
        self.async_write_ha_state()

    async def async_added_to_hass(self):
        self.async_on_remove(
            self.coordinator.async_add_listener(self._handle_coordinator_update)
        )

class UserStatisticsSensor(SensorEntity):
    """User statistics sensor."""
    
    def __init__(self, coordinator: PillsDataCoordinator, config_entry: ConfigEntry, user_id: str):
        self.coordinator = coordinator
        self.config_entry = config_entry
        self.user_id = user_id
        self._attr_unique_id = f"{config_entry.entry_id}_user_{user_id}_statistics"

    @property
    def name(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        username = user_data.get('username', f'User_{self.user_id}')
        return f"{username} Pills Statistics"

    @property
    def icon(self):
        return "mdi:account-pill"

    @property
    def device_info(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        username = user_data.get('username', f'User_{self.user_id}')
        return {
            "identifiers": {(DOMAIN, f"{self.config_entry.entry_id}_user_{self.user_id}")},
            "name": f"Pills: {username}",
            "manufacturer": "Pills Reminder Bot",
            "model": "User Pills Tracker",
            "sw_version": "1.0",
            "via_device": (DOMAIN, self.config_entry.entry_id),
        }

    @property
    def state(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        stats = user_data.get('stats', {})
        return stats.get('taken_today', 0)

    @property
    def extra_state_attributes(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        stats = user_data.get('stats', {})
        username = user_data.get('username', f'User_{self.user_id}')
        return {
            'username': username,
            'taken_today': stats.get('taken_today', 0),
            'skipped_today': stats.get('skipped_today', 0),
            'taken_week': stats.get('taken_week', 0),
            'skipped_week': stats.get('skipped_week', 0),
            'compliance_week': stats.get('compliance_week', 100),
            'active_reminders': stats.get('active_reminders', 0),
            'total_pills': len(user_data.get('pills', {})),
            'last_updated': self.coordinator.data.get('last_updated'),
            'friendly_name': f'{username} - принято сегодня',
            'unit_of_measurement': 'pills'
        }

    @callback
    def _handle_coordinator_update(self):
        self.async_write_ha_state()

    async def async_added_to_hass(self):
        self.async_on_remove(
            self.coordinator.async_add_listener(self._handle_coordinator_update)
        )

class UserComplianceSensor(SensorEntity):
    """User compliance sensor."""
    
    def __init__(self, coordinator: PillsDataCoordinator, config_entry: ConfigEntry, user_id: str):
        self.coordinator = coordinator
        self.config_entry = config_entry
        self.user_id = user_id
        self._attr_unique_id = f"{config_entry.entry_id}_user_{user_id}_compliance"
        self._attr_device_class = None

    @property
    def name(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        username = user_data.get('username', f'User_{self.user_id}')
        return f"{username} Compliance"

    @property
    def icon(self):
        return "mdi:chart-line"

    @property
    def native_unit_of_measurement(self):
        return "%"

    @property
    def device_info(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        username = user_data.get('username', f'User_{self.user_id}')
        return {
            "identifiers": {(DOMAIN, f"{self.config_entry.entry_id}_user_{self.user_id}")},
            "name": f"Pills: {username}",
            "manufacturer": "Pills Reminder Bot",
            "model": "User Pills Tracker",
            "sw_version": "1.0",
            "via_device": (DOMAIN, self.config_entry.entry_id),
        }

    @property
    def state(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        stats = user_data.get('stats', {})
        return stats.get('compliance_week', 100)

    @property
    def extra_state_attributes(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        stats = user_data.get('stats', {})
        username = user_data.get('username', f'User_{self.user_id}')
        return {
            'username': username,
            'taken_week': stats.get('taken_week', 0),
            'skipped_week': stats.get('skipped_week', 0),
            'total_week': stats.get('taken_week', 0) + stats.get('skipped_week', 0),
            'last_updated': self.coordinator.data.get('last_updated'),
            'friendly_name': f'{username} - соблюдение режима за неделю (%)'
        }

    @callback
    def _handle_coordinator_update(self):
        self.async_write_ha_state()

    async def async_added_to_hass(self):
        self.async_on_remove(
            self.coordinator.async_add_listener(self._handle_coordinator_update)
        )

class UserPillSensor(SensorEntity):
    """Individual user pill sensor."""
    
    def __init__(self, coordinator: PillsDataCoordinator, config_entry: ConfigEntry, user_id: str, pill_name: str, sensor_type: str):
        self.coordinator = coordinator
        self.config_entry = config_entry
        self.user_id = user_id
        self.pill_name = pill_name
        self.sensor_type = sensor_type
        
        # Создаем безопасное имя для ID
        safe_pill_name = pill_name.replace(' ', '_').replace('-', '_').lower()
        self._attr_unique_id = f"{config_entry.entry_id}_user_{user_id}_{safe_pill_name}_{sensor_type}"
        
        # Настройки для разных типов сенсоров
        self._setup_sensor_config()

    def _setup_sensor_config(self):
        """Setup sensor configuration based on type."""
        type_configs = {
            'taken_today': {
                'icon': "mdi:pill",
                'unit': 'times',
                'device_class': None,
                'suffix': 'принято сегодня'
            },
            'taken_week': {
                'icon': "mdi:pill-multiple",
                'unit': 'times',
                'device_class': None,
                'suffix': 'принято за неделю'
            },
            'skipped_today':  {
                'icon': "mdi:pill-off",
                'unit': 'times',
                'device_class': None,
                'suffix': 'пропущено сегодня'
            },
            'skipped_week': {
                'icon': "mdi:pill-off",
                'unit': 'times',
                'device_class': None,
                'suffix': 'пропущено за неделю'
            },
            'compliance_week': {
                'icon': "mdi:chart-line",
                'unit': '%',
                'device_class': None,
                'suffix': 'соблюдение за неделю'
            },
            'last_taken': {
                'icon': "mdi:clock-outline",
                'unit': None,
                'device_class': SensorDeviceClass.TIMESTAMP,
                'suffix': 'последний прием'
            },
            'next_due': {
                'icon': "mdi:clock-alert-outline",
                'unit': None,
                'device_class': SensorDeviceClass.TIMESTAMP,
                'suffix': 'следующий прием'
            },
            'course_progress': {
                'icon': "mdi:progress-clock",
                'unit': '%',
                'device_class': None,
                'suffix': 'прогресс курса'
            }
        }
        
        config = type_configs.get(self.sensor_type, {})
        self._attr_icon = config.get('icon', "mdi:pill")
        self._attr_native_unit_of_measurement = config.get('unit')
        self._attr_device_class = config.get('device_class')
        self._suffix = config.get('suffix', self.sensor_type)

    @property
    def name(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        username = user_data.get('username', f'User_{self.user_id}')
        return f"{username} {self.pill_name} {self.sensor_type.replace('_', ' ').title()}"

    @property
    def device_info(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        username = user_data.get('username', f'User_{self.user_id}')
        safe_pill_name = self.pill_name.replace(' ', '_').replace('-', '_').lower()
        return {
            "identifiers": {(DOMAIN, f"{self.config_entry.entry_id}_user_{self.user_id}_{safe_pill_name}")},
            "name": f"{username}: {self.pill_name}",
            "manufacturer": "Pills Reminder Bot",
            "model": "Pill Sensor",
            "sw_version": "1.0",
            "via_device": (DOMAIN, f"{self.config_entry.entry_id}_user_{self.user_id}"),
        }

    @property
    def state(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        pill_data = user_data.get('pills', {}).get(self.pill_name, {})
        
        if self.sensor_type in ['last_taken', 'next_due']:
            timestamp = pill_data.get(self.sensor_type)
            if timestamp:
                return datetime.fromisoformat(timestamp).isoformat()
            return None
        elif self.sensor_type == 'course_progress':
            progress_data = pill_data.get(self.sensor_type, {})
            return progress_data.get('progress_percent')
        
        return pill_data.get(self.sensor_type, 0)

    @property
    def extra_state_attributes(self):
        user_data = self.coordinator.data.get('users', {}).get(self.user_id, {})
        pill_data = user_data.get('pills', {}).get(self.pill_name, {})
        username = user_data.get('username', f'User_{self.user_id}')
        
        base_attrs = {
            'username': username,
            'pill_name': self.pill_name,
            'user_id': self.user_id,
            'friendly_name': f'{username} - {self.pill_name} - {self._suffix}',
            'last_updated': self.coordinator.data.get('last_updated'),
        }
        
        if self.sensor_type == 'compliance_week':
            base_attrs.update({
                'taken_week': pill_data.get('taken_week', 0),
                'total_week': pill_data.get('total_week', 0),
            })
        elif self.sensor_type in ['last_taken', 'next_due']:
            base_attrs.update({
                'taken_today': pill_data.get('taken_today', 0),
                'taken_week': pill_data.get('taken_week', 0),
            })
        elif self.sensor_type == 'course_progress':
            progress_data = pill_data.get(self.sensor_type, {})
            base_attrs.update({
                'days_passed': progress_data.get('days_passed', 0),
                'total_days': progress_data.get('total_days'),
                'days_left': progress_data.get('days_left'),
                'progress_percent': progress_data.get('progress_percent'),
            })
        
        return base_attrs

    @callback
    def _handle_coordinator_update(self):
        self.async_write_ha_state()

    async def async_added_to_hass(self):
        self.async_on_remove(
            self.coordinator.async_add_listener(self._handle_coordinator_update)
        )