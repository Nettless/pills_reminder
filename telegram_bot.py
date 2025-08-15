import asyncio
import json
import logging
from datetime import datetime, timedelta
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers import device_registry as dr, entity_registry as er
from .const import *

_LOGGER = logging.getLogger(__name__)

class PillsReminderBot:
    def __init__(self, hass: HomeAssistant, config: dict):
        self.hass = hass
        self.config = config
        self.session = async_get_clientsession(hass)
        self.base_url = f"https://api.telegram.org/bot{config[CONF_BOT_TOKEN]}"
        self.storage = Store(hass, 1, f"pills_reminder_global")
        self.users_storage = Store(hass, 1, f"pills_reminder_users")
        self.archive_storage = Store(hass, 1, f"pills_reminder_archive")
        self.reminder_task = None
        self.webhook_task = None
        self.active_reminders = {}
        
    async def start(self):
        try:
            await self.setup_bot_commands()
            self.webhook_task = self.hass.async_create_task(self.poll_updates())
            self.reminder_task = self.hass.async_create_task(self.reminder_scheduler())
            _LOGGER.info("Pills reminder bot started successfully")
        except Exception as err:
            _LOGGER.error("Failed to start telegram bot: %s", err)
            raise

    async def stop(self):
        if self.reminder_task:
            self.reminder_task.cancel()
        if self.webhook_task:
            self.webhook_task.cancel()

    async def setup_bot_commands(self):
        url = f"{self.base_url}/setMyCommands"
        commands = [
            {"command": "start", "description": "Начать использование бота"},
            {"command": "setup", "description": "Настроить новое напоминание"},
            {"command": "manage", "description": "Управление напоминаниями"},
            {"command": "status", "description": "Показать все напоминания"},
            {"command": "history", "description": "История активных витаминок"},
            {"command": "archive", "description": "Архив завершенных курсов"},
            {"command": "cleanup", "description": "Очистка истории и данных"},
            {"command": "stop", "description": "Остановить все напоминания"},
            {"command": "help", "description": "Помощь"}
        ]
        async with self.session.post(url, json={"commands": commands}) as response:
            return await response.json()

    async def poll_updates(self):
        offset = 0
        while True:
            try:
                url = f"{self.base_url}/getUpdates"
                params = {"offset": offset, "timeout": 10}
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            for update in data.get("result", []):
                                offset = max(offset, update["update_id"] + 1)
                                await self.handle_update(update)
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error polling updates: %s", err)
                await asyncio.sleep(10)

    async def handle_update(self, update):
        try:
            if "message" in update:
                await self.handle_message(update["message"])
            elif "callback_query" in update:
                await self.handle_callback_query(update["callback_query"])
        except Exception as err:
            _LOGGER.error("Error handling update: %s", err)

    async def handle_message(self, message):
        if "text" not in message:
            return
        text = message["text"]
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]
        chat_type = message["chat"]["type"]

        # В канале/группе обрабатываем только callback-и от кнопок
        if str(chat_id) == str(self.config[CONF_CHAT_ID]):
            return

        # Личные сообщения - обрабатываем команды и настройку
        if chat_type == "private":
            if text.startswith("/"):
                await self.handle_private_command(message)
            else:
                await self.handle_private_message(message)

    async def handle_private_command(self, message):
        text = message["text"]
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]

        if text.startswith("/start"):
            await self.handle_start_command(chat_id, user_id, message["from"])
        elif text.startswith("/setup"):
            await self.handle_setup_command(chat_id, user_id, message["from"])
        elif text.startswith("/manage"):
            await self.handle_manage_command(chat_id, user_id)
        elif text.startswith("/status"):
            await self.handle_status_command(chat_id, user_id)
        elif text.startswith("/history"):
            await self.handle_history_command(chat_id, user_id)
        elif text.startswith("/archive"):
            await self.handle_archive_command(chat_id, user_id)
        elif text.startswith("/cleanup"):
            await self.handle_cleanup_command(chat_id, user_id)
        elif text.startswith("/stop"):
            await self.handle_stop_command(chat_id, user_id)
        elif text.startswith("/help"):
            await self.handle_help_command(chat_id)

    async def handle_start_command(self, chat_id, user_id, user_info):
        username = user_info.get("username", user_info.get("first_name", "пользователь"))
        text = f"Привет, {username}! 👋\n\n"
        text += "Я бот для напоминания о приеме витаминок.\n\n"
        text += "🔧 Доступные команды:\n"
        text += "/setup - создать новое напоминание\n"
        text += "/manage - управление напоминаниями\n"
        text += "/status - показать все напоминания\n"
        text += "/history - история активных витаминок\n"
        text += "/archive - архив завершенных курсов\n"
        text += "/cleanup - очистка истории и данных\n"
        text += "/stop - остановить все напоминания\n"
        text += "/help - подробная помощь\n\n"
        text += "💡 Настройка происходит здесь, в личных сообщениях.\n"
        text += "📢 Напоминания будут приходить в канал с кнопками."
        await self.send_message(chat_id, text)

    async def handle_setup_command(self, chat_id, user_id, user_info):
        username = user_info.get("username", user_info.get("first_name", "пользователь"))
        users_data = await self.users_storage.async_load() or {}
        
        if str(user_id) not in users_data:
            users_data[str(user_id)] = {
                "username": username,
                "first_name": user_info.get("first_name", ""),
                "chat_id": chat_id,
                "reminders": {}
            }
        else:
            # Обновляем username если он изменился
            users_data[str(user_id)]["username"] = username
            users_data[str(user_id)]["first_name"] = user_info.get("first_name", "")
            users_data[str(user_id)]["chat_id"] = chat_id

        # Создаем новый ID для напоминания
        reminder_id = str(int(datetime.now().timestamp()))
        users_data[str(user_id)]["setup_step"] = "pill_name"
        users_data[str(user_id)]["current_reminder_id"] = reminder_id
        await self.users_storage.async_save(users_data)

        text = "🆕 Создание нового напоминания\n\n"
        text += "Шаг 1 из 7: Введите название витаминки\n"
        text += "Например: Витамин D, Омега-3, Магний и т.д."
        await self.send_message(chat_id, text)

    async def handle_manage_command(self, chat_id, user_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if not user_data or not user_data.get("reminders"):
            text = "❌ У вас нет настроенных напоминаний\n\n"
            text += "Используйте /setup для создания первого напоминания"
            await self.send_message(chat_id, text)
            return

        text = "⚙️ Управление напоминаниями:\n\n"
        keyboard_buttons = []
        
        for reminder_id, reminder in user_data["reminders"].items():
            status_icon = "🟢" if reminder.get("active", True) else "🔴"
            pill_name = reminder.get("pill_name", "Без названия")
            
            # Показываем время первого приема или все времена
            times_display = []
            for i, time_slot in enumerate(reminder.get("times", [])):
                times_display.append(time_slot.get("time", "??:??"))
            times_str = ", ".join(times_display) if times_display else "Не указано"
            
            dosage = reminder.get("dosage", "")
            course_number = reminder.get("course_number", 1)
            duration_days = reminder.get("duration_days", "∞")
            
            display_text = f"{pill_name}"
            if dosage:
                display_text += f" ({dosage})"
            if course_number > 1:
                display_text += f" [Курс #{course_number}]"
                
            text += f"{status_icon} {display_text}\n"
            text += f"    ⏰ Время: {times_str}\n"
            text += f"    📅 Длительность: {duration_days} дн.\n"
            
            if reminder.get("description"):
                text += f"    💡 {reminder['description']}\n"
            text += "\n"

            # Добавляем кнопки для каждого напоминания
            keyboard_buttons.extend([
                [{"text": f"⏰ Время {pill_name}", "callback_data": f"edit_reminder_{reminder_id}"}],
                [{"text": f"{'⏸️ Приостановить' if reminder.get('active', True) else '▶️ Включить'} {pill_name}",
                  "callback_data": f"toggle_reminder_{reminder_id}"}],
                [{"text": f"✅ Курс завершен {pill_name}", "callback_data": f"archive_reminder_{reminder_id}"}],
            ])
        
        keyboard_buttons.append([{"text": "🆕 Создать новое напоминание", "callback_data": "new_reminder"}])
        keyboard = {"inline_keyboard": keyboard_buttons}
        await self.send_message(chat_id, text, keyboard)

    async def handle_private_message(self, message):
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]
        text = message["text"]
        
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if not user_data or "setup_step" not in user_data:
            text_response = "Используйте команду /setup для создания напоминания или /manage для управления существующими"
            await self.send_message(chat_id, text_response)
            return

        setup_step = user_data["setup_step"]
        reminder_id = user_data.get("current_reminder_id")

        if setup_step == "pill_name":
            if "reminders" not in user_data:
                user_data["reminders"] = {}
            
            # Проверяем, есть ли уже курсы этой витаминки в архиве
            course_number = await self.get_next_course_number(user_id, text)
            
            user_data["reminders"][reminder_id] = {
                "pill_name": text,
                "course_number": course_number,
                "created": datetime.now().isoformat()
            }
            user_data["setup_step"] = "dosage"
            await self.users_storage.async_save(users_data)
            
            response = f"✅ Витаминка '{text}' сохранена!"
            if course_number > 1:
                response += f" (Курс #{course_number})"
            response += "\n\nШаг 2 из 7: Введите дозировку\n"
            response += "Например: 1000 МЕ, 2 таблетки, 1 капсула\n"
            response += "Или отправьте '-' чтобы пропустить"
            await self.send_message(chat_id, response)

        elif setup_step == "dosage":
            dosage = text if text != "-" else ""
            user_data["reminders"][reminder_id]["dosage"] = dosage
            user_data["setup_step"] = "description"
            await self.users_storage.async_save(users_data)
            
            response = "✅ Дозировка сохранена!\n\n"
            response += "Шаг 3 из 7: Введите описание (для чего принимаете)\n"
            response += "Например: для иммунитета, для сердца, от врача\n"
            response += "Или отправьте '-' чтобы пропустить"
            await self.send_message(chat_id, response)

        elif setup_step == "description":
            description = text if text != "-" else ""
            user_data["reminders"][reminder_id]["description"] = description
            user_data["setup_step"] = "duration_days"
            await self.users_storage.async_save(users_data)
            
            response = "✅ Описание сохранено!\n\n"
            response += "Шаг 4 из 7: Введите длительность курса в днях\n"
            response += "Например: 30, 60, 90\n"
            response += "Или отправьте '-' для бесконечного курса"
            await self.send_message(chat_id, response)

        elif setup_step == "duration_days":
            if text == "-":
                duration_days = None
            else:
                try:
                    duration_days = int(text)
                    if duration_days <= 0:
                        await self.send_message(chat_id, "❌ Длительность должна быть положительным числом!")
                        return
                except ValueError:
                    await self.send_message(chat_id, "❌ Введите число или '-' для бесконечного курса!")
                    return
            
            user_data["reminders"][reminder_id]["duration_days"] = duration_days
            user_data["setup_step"] = "times_per_day"
            await self.users_storage.async_save(users_data)
            
            response = "✅ Длительность сохранена!\n\n"
            response += "Шаг 5 из 7: Сколько раз в день принимать?\n"
            response += "Введите число от 1 до 6"
            await self.send_message(chat_id, response)

        elif setup_step == "times_per_day":
            try:
                times_per_day = int(text)
                if times_per_day < 1 or times_per_day > 6:
                    await self.send_message(chat_id, "❌ Количество приемов должно быть от 1 до 6!")
                    return
            except ValueError:
                await self.send_message(chat_id, "❌ Введите число от 1 до 6!")
                return
            
            user_data["reminders"][reminder_id]["times_per_day"] = times_per_day
            user_data["reminders"][reminder_id]["times"] = []
            user_data["setup_step"] = "time_1"
            user_data["current_time_index"] = 0
            await self.users_storage.async_save(users_data)
            
            response = f"✅ Количество приемов: {times_per_day}\n\n"
            response += "Шаг 6 из 7: Введите время приемов\n\n"
            response += f"Время 1-го приема (ЧЧ:ММ):"
            await self.send_message(chat_id, response)

        elif setup_step.startswith("time_"):
            try:
                datetime.strptime(text, "%H:%M")
            except ValueError:
                await self.send_message(chat_id, "❌ Неверный формат времени! Используйте ЧЧ:ММ")
                return
            
            current_time_index = user_data.get("current_time_index", 0)
            times_per_day = user_data["reminders"][reminder_id]["times_per_day"]
            
            # Добавляем время
            user_data["reminders"][reminder_id]["times"].append({"time": text})
            current_time_index += 1
            
            if current_time_index < times_per_day:
                # Запрашиваем следующее время
                user_data["setup_step"] = f"time_{current_time_index + 1}"
                user_data["current_time_index"] = current_time_index
                await self.users_storage.async_save(users_data)
                
                response = f"✅ Время {current_time_index}-го приема сохранено!\n\n"
                response += f"Время {current_time_index + 1}-го приема (ЧЧ:ММ):"
                await self.send_message(chat_id, response)
            else:
                # Все времена собраны, переходим к подтверждению
                user_data["setup_step"] = "confirm"
                user_data.pop("current_time_index", None)
                await self.users_storage.async_save(users_data)
                
                await self.show_confirmation(chat_id, user_id, reminder_id)

        elif setup_step == "edit_times":
            # Редактирование времен существующего напоминания
            times_text = text.strip().replace(" ", "").split(",")
            times_list = []
            
            try:
                for time_str in times_text:
                    datetime.strptime(time_str, "%H:%M")
                    times_list.append({"time": time_str})
            except ValueError:
                await self.send_message(chat_id, "❌ Неверный формат! Используйте: ЧЧ:ММ,ЧЧ:ММ,...")
                return
            
            editing_reminder_id = user_data.get("editing_reminder_id")
            if editing_reminder_id and editing_reminder_id in user_data.get("reminders", {}):
                user_data["reminders"][editing_reminder_id]["times"] = times_list
                user_data["reminders"][editing_reminder_id]["times_per_day"] = len(times_list)
                user_data.pop("setup_step", None)
                user_data.pop("editing_reminder_id", None)
                await self.users_storage.async_save(users_data)
                
                reminder = user_data["reminders"][editing_reminder_id]
                times_display = [t["time"] for t in times_list]
                
                response = "✅ Времена приема обновлены!\n\n"
                response += f"💊 Витаминка: {reminder['pill_name']}\n"
                response += f"⏰ Времена: {', '.join(times_display)}"
                await self.send_message(chat_id, response)

    async def show_confirmation(self, chat_id, user_id, reminder_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        reminder = user_data["reminders"][reminder_id]
        
        response = "✅ Все данные собраны!\n\n"
        response += f"📋 Проверьте настройки:\n"
        response += f"💊 Витаминка: {reminder['pill_name']}"
        if reminder.get('course_number', 1) > 1:
            response += f" (Курс #{reminder['course_number']})"
        response += "\n"
        
        if reminder.get('dosage'):
            response += f"📏 Дозировка: {reminder['dosage']}\n"
        if reminder.get('description'):
            response += f"💡 Описание: {reminder['description']}\n"
            
        duration = reminder.get('duration_days')
        if duration:
            response += f"📅 Длительность: {duration} дней\n"
        else:
            response += f"📅 Длительность: бесконечно\n"
            
        times_display = [t["time"] for t in reminder.get("times", [])]
        response += f"⏰ Времена приема: {', '.join(times_display)}\n"
        response += f"👤 Пользователь: @{user_data.get('username', user_data.get('first_name', 'безымянный'))}\n\n"
        response += "📢 Напоминания будут приходить в канал с кнопками для ответа"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "✅ Сохранить напоминание", "callback_data": f"save_reminder_{reminder_id}"}],
                [{"text": "❌ Отменить", "callback_data": f"cancel_reminder_{reminder_id}"}]
            ]
        }
        await self.send_message(chat_id, response, keyboard)

    async def get_next_course_number(self, user_id, pill_name):
        """Получает номер следующего курса для данной витаминки (только родительские курсы)"""
        try:
            # Проверяем активные напоминания - только курс #1
            users_data = await self.users_storage.async_load() or {}
            user_data = users_data.get(str(user_id), {})
            max_course = 0
            
            # Ищем в активных напоминаниях - только курс #1
            for reminder in user_data.get("reminders", {}).values():
                if (reminder.get("pill_name") == pill_name and 
                    reminder.get("course_number", 1) == 1):
                    max_course = 1
            
            # Ищем в архиве - только курс #1
            archive_data = await self.archive_storage.async_load() or {'archive': []}
            for entry in archive_data.get('archive', []):
                if (entry.get('user_id') == str(user_id) and
                    entry.get('reminder_data', {}).get('pill_name') == pill_name and
                    entry.get('reminder_data', {}).get('course_number', 1) == 1):
                    max_course = max(max_course, entry.get('reminder_data', {}).get('course_number', 1))
            
            return max_course + 1
        except Exception as err:
            _LOGGER.error(f"Error getting next course number: {err}")
            return 1

    async def handle_status_command(self, chat_id, user_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if not user_data or not user_data.get("reminders"):
            text = "❌ У вас нет настроенных напоминаний\n\n"
            text += "Используйте /setup для создания напоминания"
            await self.send_message(chat_id, text)
            return

        text = "📋 Ваши напоминания:\n\n"
        active_count = 0
        inactive_count = 0
        
        for reminder_id, reminder in user_data["reminders"].items():
            is_active = reminder.get("active", True)
            status_icon = "🟢" if is_active else "🔴"
            status_text = "Активно" if is_active else "Приостановлено"
            course_number = reminder.get("course_number", 1)
            duration_days = reminder.get("duration_days")
            
            text += f"{status_icon} {reminder.get('pill_name', 'Без названия')}"
            if reminder.get('dosage'):
                text += f" ({reminder['dosage']})"
            if course_number > 1:
                text += f" [Курс #{course_number}]"
            text += "\n"
            
            times_display = [t["time"] for t in reminder.get("times", [])]
            text += f"   ⏰ Времена: {', '.join(times_display) if times_display else 'не указано'}\n"
            
            if duration_days:
                # Вычисляем прогресс курса
                start_date = datetime.fromisoformat(reminder.get('created', datetime.now().isoformat()))
                days_passed = (datetime.now() - start_date).days + 1
                days_left = max(0, duration_days - days_passed + 1)
                text += f"   📅 Прогресс: {days_passed}/{duration_days} дн. (осталось {days_left} дн.)\n"
            else:
                text += f"   📅 Длительность: бесконечно\n"
                
            if reminder.get('description'):
                text += f"   💡 Описание: {reminder['description']}\n"
            text += f"   📊 Статус: {status_text}\n\n"
            
            if is_active:
                active_count += 1
            else:
                inactive_count += 1

        text += f"📊 Итого: {active_count} активных, {inactive_count} приостановленных\n"
        text += f"📢 Напоминания приходят в канал\n\n"
        text += f"💡 Используйте /archive для просмотра завершенных курсов"
        await self.send_message(chat_id, text)

    async def handle_stop_command(self, chat_id, user_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if not user_data or not user_data.get("reminders"):
            text = "❌ У вас нет активных напоминаний"
            await self.send_message(chat_id, text)
            return

        # Останавливаем все напоминания
        stopped_count = 0
        for reminder_id, reminder in user_data["reminders"].items():
            if reminder.get("active", True):
                reminder["active"] = False
                stopped_count += 1
        
        await self.users_storage.async_save(users_data)

        # Убираем активные напоминания
        reminders_to_remove = [rid for rid in self.active_reminders.keys() if rid.startswith(f"{user_id}_")]
        for reminder_key in reminders_to_remove:
            del self.active_reminders[reminder_key]

        text = f"🔴 Остановлено {stopped_count} напоминаний\n\n"
        text += "Используйте /manage для управления напоминаниями"
        await self.send_message(chat_id, text)

    async def handle_history_command(self, chat_id, user_id):
        history_text = await self.get_user_history(user_id, active_only=True)
        await self.send_message(chat_id, history_text)

    async def handle_archive_command(self, chat_id, user_id):
        archive_text = await self.get_user_archive(user_id)
        
        # Добавляем кнопки для повтора курсов - только родительские курсы (#1)
        archive_data = await self.archive_storage.async_load() or {'archive': []}
        user_archive = [
            entry for entry in archive_data.get('archive', [])
            if (entry.get('user_id') == str(user_id) and 
                entry.get('reminder_data', {}).get('course_number', 1) == 1)
        ]
        
        keyboard_buttons = []
        if user_archive:
            for entry in user_archive[-5:]:  # Показываем последние 5 родительских курсов для повтора
                reminder_data = entry.get('reminder_data', {})
                pill_name = reminder_data.get('pill_name', 'Курс')
                button_text = f"🔄 Повторить {pill_name}"
                keyboard_buttons.append([{
                    "text": button_text,
                    "callback_data": f"repeat_course_{entry.get('archived_at', '')}"
                }])
        
        if keyboard_buttons:
            keyboard = {"inline_keyboard": keyboard_buttons}
            await self.send_message(chat_id, archive_text, keyboard)
        else:
            await self.send_message(chat_id, archive_text)

    async def handle_cleanup_command(self, chat_id, user_id):
        """Обработка команды очистки истории"""
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        # Проверяем, есть ли что очищать
        history_data = await self.storage.async_load() or {'history': []}
        archive_data = await self.archive_storage.async_load() or {'archive': []}
        user_history = [entry for entry in history_data.get('history', []) if entry.get('user_id') == str(user_id)]
        user_archive = [entry for entry in archive_data.get('archive', []) if entry.get('user_id') == str(user_id)]
        
        if not user_history and not user_archive and not (user_data and user_data.get('reminders')):
            text = "❌ У вас нет данных для очистки"
            await self.send_message(chat_id, text)
            return

        text = "🧹 Очистка истории и данных\n\n"
        text += "⚠️ ВНИМАНИЕ! Очистка необратима!\n\n"
        text += "Что будет удалено:\n"
        if user_history:
            text += f"📊 Активная история: {len(user_history)} записей\n"
        if user_archive:
            text += f"🗄️ Архив: {len(user_archive)} курсов\n"
        if user_data and user_data.get('reminders'):
            text += f"⚙️ Активные напоминания: {len(user_data['reminders'])}\n"
        text += f"🏠 Устройства и сенсоры в Home Assistant\n\n"
        text += "Выберите, что очистить:"

        keyboard_buttons = [
            [{"text": "🧹 Очистить ВСЁ", "callback_data": f"cleanup_all_{user_id}"}],
        ]
        
        # Добавляем кнопки для очистки отдельных курсов
        if user_archive or (user_data and user_data.get('reminders')):
            keyboard_buttons.append([{"text": "📋 Выборочная очистка", "callback_data": f"cleanup_selective_{user_id}"}])
        
        keyboard_buttons.append([{"text": "❌ Отмена", "callback_data": "cleanup_cancel"}])
        keyboard = {"inline_keyboard": keyboard_buttons}
        await self.send_message(chat_id, text, keyboard)

    async def handle_help_command(self, chat_id):
        text = "🆘 Справка по боту Pills Reminder\n\n"
        text += "🔧 Команды для управления:\n"
        text += "/setup - создать новое напоминание\n"
        text += "/manage - управление напоминаниями\n"
        text += "/status - список всех напоминаний\n"
        text += "/history - история активных витаминок\n"
        text += "/archive - архив завершенных курсов\n"
        text += "/cleanup - очистка истории и данных\n"
        text += "/stop - остановить все напоминания\n\n"
        text += "💡 Как это работает:\n"
        text += "1️⃣ Настраивайте бота здесь, в личных сообщениях\n"
        text += "2️⃣ Можно создать несколько напоминаний для разных витаминок\n"
        text += "3️⃣ Для каждой витаминки можно настроить до 6 приемов в день\n"
        text += "4️⃣ В указанное время в канал придут напоминания с прогрессом курса\n"
        text += "5️⃣ Нажимайте кнопки в канале: ✅ Выпил, ❌ Пропустить или 📝 Описание\n"
        text += "6️⃣ Если не ответите, напоминания повторяются каждые 30 минут\n"
        text += "7️⃣ Управляйте напоминаниями через /manage\n\n"
        text += "⚙️ В меню управления можно:\n"
        text += "• Изменять время приемов\n"
        text += "• Приостанавливать/включать напоминания\n"
        text += "• Завершать курс (переносить в архив)\n\n"
        text += "📊 /history - показывает только активные витаминки\n"
        text += "🗄️ /archive - показывает все завершенные курсы с возможностью повтора\n"
        text += "🧹 /cleanup - очистка истории и данных (включая сенсоры Home Assistant)\n\n"
        text += "🔄 Повтор курсов: можно повторять только родительские курсы (Курс #1)"
        await self.send_message(chat_id, text)

    async def handle_callback_query(self, callback_query):
        await self.answer_callback_query(callback_query["id"])
        data = callback_query["data"]
        chat_id = callback_query["message"]["chat"]["id"]
        user_id = callback_query["from"]["id"]
        message_id = callback_query["message"]["message_id"]

        # Создание и настройка напоминаний
        if data.startswith("save_reminder_"):
            reminder_id = data.split("_")[2]
            await self.save_reminder(chat_id, user_id, message_id, reminder_id)
        elif data.startswith("cancel_reminder_"):
            reminder_id = data.split("_")[2]
            await self.cancel_reminder(chat_id, user_id, message_id, reminder_id)
        elif data == "new_reminder":
            await self.start_new_reminder(chat_id, user_id, message_id)

        # Управление напоминаниями
        elif data.startswith("edit_reminder_"):
            reminder_id = data.split("_")[2]
            await self.start_edit_reminder(chat_id, user_id, message_id, reminder_id)
        elif data.startswith("toggle_reminder_"):
            reminder_id = data.split("_")[2]
            await self.toggle_reminder(chat_id, user_id, message_id, reminder_id)
        elif data.startswith("archive_reminder_"):
            reminder_id = data.split("_")[2]
            await self.confirm_archive_reminder(chat_id, user_id, message_id, reminder_id)
        elif data.startswith("confirm_archive_"):
            reminder_id = data.split("_")[2]
            await self.archive_reminder(chat_id, user_id, message_id, reminder_id)
        elif data.startswith("cancel_archive_"):
            await self.handle_manage_command(chat_id, user_id)

        # Повтор курса из архива
        elif data.startswith("repeat_course_"):
            archived_at = data.split("_", 2)[2]
            await self.repeat_course_from_archive(chat_id, user_id, message_id, archived_at)

        # Очистка данных
        elif data.startswith("cleanup_all_"):
            cleanup_user_id = data.split("_")[2]
            await self.cleanup_all_data(chat_id, user_id, message_id, cleanup_user_id)
        elif data.startswith("cleanup_selective_"):
            cleanup_user_id = data.split("_")[2]
            await self.show_selective_cleanup(chat_id, user_id, message_id, cleanup_user_id)
        elif data.startswith("cleanup_pill_"):
            parts = data.split("_", 3)
            cleanup_user_id = parts[2]
            pill_name = parts[3]
            await self.cleanup_pill_data(chat_id, user_id, message_id, cleanup_user_id, pill_name)
        elif data.startswith("confirm_cleanup_pill_"):
            parts = data.split("_", 4)
            cleanup_user_id = parts[3]
            pill_name = parts[4]
            await self.confirm_cleanup_pill_data(chat_id, user_id, message_id, cleanup_user_id, pill_name)
        elif data == "cleanup_cancel":
            await self.edit_message_text(chat_id, message_id, "❌ Очистка отменена")

        # Действия с напоминаниями в канале
        elif data.startswith("taken_"):
            parts = data.split("_")
            reminder_user_id = parts[1]
            reminder_id = parts[2] if len(parts) > 2 else "default"
            time_index = int(parts[3]) if len(parts) > 3 else 0
            await self.mark_as_taken(chat_id, reminder_user_id, message_id, user_id, reminder_id, time_index)
        elif data.startswith("skip_"):
            parts = data.split("_")
            reminder_user_id = parts[1]
            reminder_id = parts[2] if len(parts) > 2 else "default"
            time_index = int(parts[3]) if len(parts) > 3 else 0
            await self.mark_as_skipped(chat_id, reminder_user_id, message_id, user_id, reminder_id, time_index)

        # Показать описание витаминки
        elif data.startswith("description_"):
            parts = data.split("_")
            reminder_user_id = parts[1]
            reminder_id = parts[2] if len(parts) > 2 else "default"
            await self.show_description(chat_id, reminder_user_id, message_id, user_id, reminder_id)

    async def cleanup_all_data(self, chat_id, user_id, message_id, cleanup_user_id):
        """Полная очистка всех данных пользователя"""
        try:
            if str(user_id) != str(cleanup_user_id):
                await self.edit_message_text(chat_id, message_id, "❌ Ошибка: неверный пользователь")
                return

            # Очищаем активную историю
            history_data = await self.storage.async_load() or {'history': []}
            user_history_count = len([entry for entry in history_data.get('history', []) if entry.get('user_id') == str(user_id)])
            history_data['history'] = [entry for entry in history_data.get('history', []) if entry.get('user_id') != str(user_id)]
            await self.storage.async_save(history_data)

            # Очищаем архив
            archive_data = await self.archive_storage.async_load() or {'archive': []}
            user_archive_count = len([entry for entry in archive_data.get('archive', []) if entry.get('user_id') == str(user_id)])
            archive_data['archive'] = [entry for entry in archive_data.get('archive', []) if entry.get('user_id') != str(user_id)]
            await self.archive_storage.async_save(archive_data)

            # Очищаем активные напоминания пользователя
            users_data = await self.users_storage.async_load() or {}
            reminders_count = 0
            if str(user_id) in users_data:
                reminders_count = len(users_data[str(user_id)].get('reminders', {}))
                del users_data[str(user_id)]
                await self.users_storage.async_save(users_data)

            # Убираем активные напоминания из памяти
            reminders_to_remove = [rid for rid in self.active_reminders.keys() if rid.startswith(f"{user_id}_")]
            for reminder_key in reminders_to_remove:
                del self.active_reminders[reminder_key]

            # Принудительная очистка устройств в HA
            await self.cleanup_ha_devices(user_id=str(user_id))

            # Обновляем сенсоры
            await self.update_sensors()

            text = "✅ Все данные очищены!\n\n"
            text += f"🗑️ Удалено:\n"
            text += f"📊 Активная история: {user_history_count} записей\n"
            text += f"🗄️ Архив: {user_archive_count} курсов\n"
            text += f"⚙️ Активные напоминания: {reminders_count}\n"
            text += f"🏠 Все устройства и сенсоры в Home Assistant\n\n"
            text += "💡 Используйте /setup для создания новых напоминаний"
            await self.edit_message_text(chat_id, message_id, text)

        except Exception as err:
            _LOGGER.error(f"Error in cleanup_all_data: {err}")
            await self.edit_message_text(chat_id, message_id, "❌ Ошибка при очистке данных")

    async def show_selective_cleanup(self, chat_id, user_id, message_id, cleanup_user_id):
        """Показывает меню выборочной очистки"""
        try:
            if str(user_id) != str(cleanup_user_id):
                await self.edit_message_text(chat_id, message_id, "❌ Ошибка: неверный пользователь")
                return

            # Получаем список всех витаминок пользователя из архива и активных напоминаний
            archive_data = await self.archive_storage.async_load() or {'archive': []}
            users_data = await self.users_storage.async_load() or {}
            user_archive = [entry for entry in archive_data.get('archive', []) if entry.get('user_id') == str(user_id)]
            user_data = users_data.get(str(user_id), {})

            # Собираем уникальные витаминки
            pills_info = {}

            # Из архива
            for entry in user_archive:
                reminder_data = entry.get('reminder_data', {})
                pill_name = reminder_data.get('pill_name', 'Неизвестно')
                if pill_name not in pills_info:
                    pills_info[pill_name] = {'archive_count': 0, 'active': False}
                pills_info[pill_name]['archive_count'] += 1

            # Из активных напоминаний
            for reminder in user_data.get('reminders', {}).values():
                pill_name = reminder.get('pill_name', 'Неизвестно')
                if pill_name not in pills_info:
                    pills_info[pill_name] = {'archive_count': 0, 'active': False}
                pills_info[pill_name]['active'] = True

            if not pills_info:
                await self.edit_message_text(chat_id, message_id, "❌ Нет данных для очистки")
                return

            text = "📋 Выборочная очистка\n\n"
            text += "Выберите витаминку для очистки:\n\n"

            keyboard_buttons = []
            for pill_name, info in pills_info.items():
                status_text = ""
                if info['active']:
                    status_text += "🟢 активна"
                if info['archive_count'] > 0:
                    if status_text:
                        status_text += f", 🗄️ архив: {info['archive_count']}"
                    else:
                        status_text += f"🗄️ архив: {info['archive_count']}"

                text += f"💊 {pill_name} ({status_text})\n"
                keyboard_buttons.append([{
                    "text": f"🗑️ Очистить {pill_name}",
                    "callback_data": f"cleanup_pill_{user_id}_{pill_name}"
                }])

            keyboard_buttons.extend([
                [{"text": "🧹 Очистить ВСЁ", "callback_data": f"cleanup_all_{user_id}"}],
                [{"text": "❌ Отмена", "callback_data": "cleanup_cancel"}]
            ])

            keyboard = {"inline_keyboard": keyboard_buttons}
            await self.edit_message_text(chat_id, message_id, text)
            await self.send_message(chat_id, text, keyboard)

        except Exception as err:
            _LOGGER.error(f"Error in show_selective_cleanup: {err}")
            await self.edit_message_text(chat_id, message_id, "❌ Ошибка при отображении меню")

    async def cleanup_pill_data(self, chat_id, user_id, message_id, cleanup_user_id, pill_name):
        """Подтверждение очистки данных конкретной витаминки"""
        try:
            if str(user_id) != str(cleanup_user_id):
                await self.edit_message_text(chat_id, message_id, "❌ Ошибка: неверный пользователь")
                return

            # Подсчитываем, что будет удалено
            history_data = await self.storage.async_load() or {'history': []}
            archive_data = await self.archive_storage.async_load() or {'archive': []}
            users_data = await self.users_storage.async_load() or {}

            user_history = [entry for entry in history_data.get('history', [])
                          if entry.get('user_id') == str(user_id) and entry.get('pill_name') == pill_name]
            user_archive = [entry for entry in archive_data.get('archive', [])
                          if entry.get('user_id') == str(user_id) and
                          entry.get('reminder_data', {}).get('pill_name') == pill_name]

            active_reminders = []
            user_data = users_data.get(str(user_id), {})
            for reminder_id, reminder in user_data.get('reminders', {}).items():
                if reminder.get('pill_name') == pill_name:
                    active_reminders.append((reminder_id, reminder))

            if not user_history and not user_archive and not active_reminders:
                await self.edit_message_text(chat_id, message_id, f"❌ Нет данных для витаминки '{pill_name}'")
                return

            text = f"🗑️ Очистка данных: {pill_name}\n\n"
            text += "⚠️ Будет удалено:\n"
            if len(user_history) > 0:
                text += f"📊 Активная история: {len(user_history)} записей\n"
            if len(user_archive) > 0:
                text += f"🗄️ Архив: {len(user_archive)} курсов\n"
            if len(active_reminders) > 0:
                text += f"⚙️ Активные напоминания: {len(active_reminders)}\n"
            text += f"🏠 Сенсоры для этой витаминки в Home Assistant\n\n"
            text += "❗ Это действие необратимо!"

            keyboard = {
                "inline_keyboard": [
                    [{"text": f"🗑️ Да, удалить {pill_name}", "callback_data": f"confirm_cleanup_pill_{user_id}_{pill_name}"}],
                    [{"text": "❌ Отмена", "callback_data": "cleanup_cancel"}]
                ]
            }
            await self.edit_message_text(chat_id, message_id, text)
            await self.send_message(chat_id, text, keyboard)

        except Exception as err:
            _LOGGER.error(f"Error in cleanup_pill_data: {err}")
            await self.edit_message_text(chat_id, message_id, "❌ Ошибка при подготовке очистки")

    async def confirm_cleanup_pill_data(self, chat_id, user_id, message_id, cleanup_user_id, pill_name):
        """Выполняет очистку данных конкретной витаминки"""
        try:
            if str(user_id) != str(cleanup_user_id):
                await self.edit_message_text(chat_id, message_id, "❌ Ошибка: неверный пользователь")
                return

            deleted_counts = {'history': 0, 'archive': 0, 'active': 0}

            # Очищаем активную историю
            history_data = await self.storage.async_load() or {'history': []}
            original_count = len(history_data.get('history', []))
            history_data['history'] = [entry for entry in history_data.get('history', [])
                                     if not (entry.get('user_id') == str(user_id) and entry.get('pill_name') == pill_name)]
            deleted_counts['history'] = original_count - len(history_data.get('history', []))
            await self.storage.async_save(history_data)

            # Очищаем архив
            archive_data = await self.archive_storage.async_load() or {'archive': []}
            original_archive_count = len(archive_data.get('archive', []))
            archive_data['archive'] = [entry for entry in archive_data.get('archive', [])
                                     if not (entry.get('user_id') == str(user_id) and
                                           entry.get('reminder_data', {}).get('pill_name') == pill_name)]
            deleted_counts['archive'] = original_archive_count - len(archive_data.get('archive', []))
            await self.archive_storage.async_save(archive_data)

            # Очищаем активные напоминания
            users_data = await self.users_storage.async_load() or {}
            user_data = users_data.get(str(user_id), {})
            reminders_to_delete = []
            
            for reminder_id, reminder in user_data.get('reminders', {}).items():
                if reminder.get('pill_name') == pill_name:
                    reminders_to_delete.append(reminder_id)

            for reminder_id in reminders_to_delete:
                del user_data['reminders'][reminder_id]
                deleted_counts['active'] += 1
                # Убираем из активных напоминаний в памяти
                reminder_key = f"{user_id}_{reminder_id}"
                if reminder_key in self.active_reminders:
                    del self.active_reminders[reminder_key]

            if str(user_id) in users_data:
                users_data[str(user_id)] = user_data
                await self.users_storage.async_save(users_data)

            # Принудительная очистка устройства витаминки в HA
            await self.cleanup_ha_devices(user_id=str(user_id), pill_name=pill_name)

            # Обновляем сенсоры
            await self.update_sensors()

            text = f"✅ Данные для '{pill_name}' очищены!\n\n"
            text += f"🗑️ Удалено:\n"
            if deleted_counts['history'] > 0:
                text += f"📊 История: {deleted_counts['history']} записей\n"
            if deleted_counts['archive'] > 0:
                text += f"🗄️ Архив: {deleted_counts['archive']} курсов\n"
            if deleted_counts['active'] > 0:
                text += f"⚙️ Активные напоминания: {deleted_counts['active']}\n"
            text += f"🏠 Устройства и сенсоры в Home Assistant\n\n"
            text += "💡 Устройства удалены автоматически"
            await self.edit_message_text(chat_id, message_id, text)

        except Exception as err:
            _LOGGER.error(f"Error in confirm_cleanup_pill_data: {err}")
            await self.edit_message_text(chat_id, message_id, "❌ Ошибка при очистке данных")

    async def cleanup_ha_devices(self, user_id=None, pill_name=None):
        """Принудительная очистка устройств в Home Assistant"""
        try:
            # Получаем доступ к реестрам
            device_registry = dr.async_get(self.hass)
            
            # Находим интеграцию
            domain_data = self.hass.data.get(DOMAIN, {})
            coordinator = None
            config_entry = None
            for entry_id, entry_data in domain_data.items():
                if isinstance(entry_data, dict) and 'coordinator' in entry_data:
                    coordinator = entry_data['coordinator']
                    config_entry = coordinator.config_entry
                    break
                    
            if not coordinator:
                _LOGGER.warning("Coordinator not found for device cleanup")
                return
            
            if user_id and pill_name:
                # Очистка устройства конкретной витаминки
                await coordinator._cleanup_pill_device(user_id, pill_name)
            elif user_id:
                # Очистка всех устройств пользователя
                await coordinator._cleanup_deleted_users([user_id])
            else:
                # Полная очистка всех неиспользуемых устройств
                users_data = await self.users_storage.async_load() or {}
                all_devices = device_registry.devices
                
                for device in all_devices.values():
                    if device.config_entries and config_entry.entry_id in device.config_entries:
                        # Проверяем, должно ли устройство существовать
                        device_should_exist = False
                        for identifier_set in device.identifiers:
                            for domain_check, device_id in identifier_set:
                                if domain_check == DOMAIN:
                                    if "_user_" in device_id:
                                        # Проверяем устройства пользователей
                                        if "_user_" in device_id and not device_id.endswith("_"):
                                            user_id_from_device = device_id.split("_user_")[1].split("_")[0]
                                            if user_id_from_device in users_data:
                                                device_should_exist = True
                                                break
                        if not device_should_exist:
                            device_registry.async_remove_device(device.id)
                            _LOGGER.info(f"Removed orphaned device: {device.name}")
            
            _LOGGER.info("Device cleanup completed")
        except Exception as err:
            _LOGGER.error(f"Error in cleanup_ha_devices: {err}")

    async def repeat_course_from_archive(self, chat_id, user_id, message_id, archived_at):
        try:
            archive_data = await self.archive_storage.async_load() or {'archive': []}
            
            # Находим архивную запись - только родительские курсы (#1)
            archive_entry = None
            for entry in archive_data.get('archive', []):
                if (entry.get('user_id') == str(user_id) and
                    entry.get('archived_at') == archived_at and
                    entry.get('reminder_data', {}).get('course_number', 1) == 1):
                    archive_entry = entry
                    break

            if not archive_entry:
                await self.edit_message_text(chat_id, message_id, "❌ Архивная запись не найдена или это не родительский курс")
                return

            # Создаем новое напоминание на основе архивного
            users_data = await self.users_storage.async_load() or {}
            user_data = users_data.get(str(user_id), {})
            
            if "reminders" not in user_data:
                user_data["reminders"] = {}

            reminder_data = archive_entry.get('reminder_data', {})
            pill_name = reminder_data.get('pill_name', 'Витаминка')

            # Получаем следующий номер курса (только для родительских курсов)
            next_course_number = await self.get_next_course_number(user_id, pill_name)

            # Создаем новое напоминание
            new_reminder_id = str(int(datetime.now().timestamp()))
            new_reminder = {
                "pill_name": pill_name,
                "dosage": reminder_data.get('dosage', ''),
                "description": reminder_data.get('description', ''),
                "duration_days": reminder_data.get('duration_days'),
                "times_per_day": reminder_data.get('times_per_day', 1),
                "times": reminder_data.get('times', [{"time": "09:00"}]),
                "course_number": next_course_number,
                "active": True,
                "created": datetime.now().isoformat()
            }

            user_data["reminders"][new_reminder_id] = new_reminder
            users_data[str(user_id)] = user_data
            await self.users_storage.async_save(users_data)

            times_display = [t["time"] for t in new_reminder.get("times", [])]
            duration_text = f"{new_reminder.get('duration_days')} дней" if new_reminder.get('duration_days') else "бесконечно"

            text = f"✅ Курс повторен!\n\n"
            text += f"💊 Витаминка: {pill_name} (Курс #{next_course_number})\n"
            if new_reminder.get('dosage'):
                text += f"📏 Дозировка: {new_reminder['dosage']}\n"
            if new_reminder.get('description'):
                text += f"💡 Описание: {new_reminder['description']}\n"
            text += f"📅 Длительность: {duration_text}\n"
            text += f"⏰ Времена приема: {', '.join(times_display)}\n\n"
            text += f"📢 Напоминания будут приходить в канал каждый день"
            
            await self.edit_message_text(chat_id, message_id, text)

            # Обновляем сенсоры
            await self.update_sensors()

        except Exception as err:
            _LOGGER.error(f"Error repeating course: {err}")
            await self.edit_message_text(chat_id, message_id, "❌ Ошибка при повторе курса")

    async def show_description(self, chat_id, reminder_user_id, message_id, action_user_id, reminder_id):
        try:
            users_data = await self.users_storage.async_load() or {}
            user_data = users_data.get(str(reminder_user_id))
            
            if not user_data:
                await self.edit_message_text(chat_id, message_id, "❌ Пользователь не найден")
                return

            # Находим информацию о витаминке
            pill_info = None
            if reminder_id != "default" and reminder_id in user_data.get("reminders", {}):
                pill_info = user_data["reminders"][reminder_id]
            elif user_data.get("reminders"):
                pill_info = next(iter(user_data["reminders"].values()))

            if not pill_info:
                await self.edit_message_text(chat_id, message_id, "❌ Информация о витаминке не найдена")
                return

            # Формируем текст с описанием
            text = f"📝 Описание витаминки\n\n"
            text += f"💊 Название: {pill_info.get('pill_name', 'Неизвестно')}\n"
            
            if pill_info.get('dosage'):
                text += f"📏 Дозировка: {pill_info['dosage']}\n"
            if pill_info.get('description'):
                text += f"💡 Для чего: {pill_info['description']}\n"
            else:
                text += f"💡 Описание не указано\n"
                
            times_display = [t["time"] for t in pill_info.get("times", [])]
            text += f"⏰ Времена приема: {', '.join(times_display) if times_display else 'не указано'}\n"
            
            if pill_info.get('course_number', 1) > 1:
                text += f"📚 Курс: #{pill_info['course_number']}\n"
                
            duration_days = pill_info.get("duration_days")
            if duration_days:
                start_date = datetime.fromisoformat(pill_info.get('created', datetime.now().isoformat()))
                days_passed = (datetime.now() - start_date).days + 1
                days_left = max(0, duration_days - days_passed + 1)
                text += f"📅 Прогресс: {days_passed}/{duration_days} дн. (осталось {days_left} дн.)\n"
            else:
                text += f"📅 Длительность: бесконечно\n"

            # Добавляем кнопки действий для всех времен приема
            keyboard_buttons = []
            for i, time_slot in enumerate(pill_info.get("times", [])):
                time_str = time_slot.get("time", "??:??")
                keyboard_buttons.extend([
                    [{"text": f"✅ Выпил в {time_str}", "callback_data": f"taken_{reminder_user_id}_{reminder_id}_{i}"}],
                    [{"text": f"❌ Пропустить {time_str}", "callback_data": f"skip_{reminder_user_id}_{reminder_id}_{i}"}]
                ])

            keyboard = {"inline_keyboard": keyboard_buttons}
            await self.edit_message_text(chat_id, message_id, text)
            await self.send_message(chat_id, text, keyboard)

        except Exception as err:
            _LOGGER.error(f"Error showing description: {err}")
            await self.edit_message_text(chat_id, message_id, "❌ Ошибка при получении описания")

    async def start_new_reminder(self, chat_id, user_id, message_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id), {})
        
        reminder_id = str(int(datetime.now().timestamp()))
        user_data["setup_step"] = "pill_name"
        user_data["current_reminder_id"] = reminder_id
        users_data[str(user_id)] = user_data
        await self.users_storage.async_save(users_data)

        text = "🆕 Создание нового напоминания\n\n"
        text += "Введите название витаминки:"
        await self.edit_message_text(chat_id, message_id, text)

    async def start_edit_reminder(self, chat_id, user_id, message_id, reminder_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if not user_data or reminder_id not in user_data.get("reminders", {}):
            await self.edit_message_text(chat_id, message_id, "❌ Напоминание не найдено")
            return

        reminder = user_data["reminders"][reminder_id]
        user_data["setup_step"] = "edit_times"
        user_data["editing_reminder_id"] = reminder_id
        await self.users_storage.async_save(users_data)

        times_display = [t["time"] for t in reminder.get("times", [])]
        
        text = f"⏰ Изменение времен приема\n\n"
        text += f"Витаминка: {reminder.get('pill_name', 'не указано')}\n"
        text += f"Текущие времена: {', '.join(times_display) if times_display else 'не указано'}\n\n"
        text += "Введите новые времена через запятую (ЧЧ:ММ,ЧЧ:ММ):"
        await self.edit_message_text(chat_id, message_id, text)

    async def toggle_reminder(self, chat_id, user_id, message_id, reminder_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if not user_data or reminder_id not in user_data.get("reminders", {}):
            await self.edit_message_text(chat_id, message_id, "❌ Напоминание не найдено")
            return

        reminder = user_data["reminders"][reminder_id]
        is_active = reminder.get("active", True)
        reminder["active"] = not is_active
        await self.users_storage.async_save(users_data)

        # Убираем из активных если отключили
        if is_active:
            reminder_key = f"{user_id}_{reminder_id}"
            if reminder_key in self.active_reminders:
                del self.active_reminders[reminder_key]

        action = "включено" if not is_active else "приостановлено"
        text = f"✅ Напоминание '{reminder['pill_name']}' {action}"
        await self.edit_message_text(chat_id, message_id, text)

        # Возвращаемся к меню управления через 2 секунды
        await asyncio.sleep(2)
        await self.handle_manage_command(chat_id, user_id)

    async def confirm_archive_reminder(self, chat_id, user_id, message_id, reminder_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if not user_data or reminder_id not in user_data.get("reminders", {}):
            await self.edit_message_text(chat_id, message_id, "❌ Напоминание не найдено")
            return

        reminder = user_data["reminders"][reminder_id]

        # Получаем статистику приема для этого курса
        history_data = await self.storage.async_load() or {'history': []}
        course_history = [
            entry for entry in history_data.get('history', [])
            if (entry.get('user_id') == str(user_id) and
                entry.get('pill_name') == reminder.get('pill_name') and
                entry.get('reminder_id') == reminder_id)
        ]

        taken_count = sum(1 for entry in course_history if entry['status'] == 'taken')
        skipped_count = sum(1 for entry in course_history if entry['status'] == 'skipped')

        text = f"🗄️ Завершение курса\n\n"
        text += f"Вы действительно хотите завершить курс?\n\n"
        text += f"💊 {reminder.get('pill_name', 'Без названия')}"
        if reminder.get('dosage'):
            text += f" ({reminder['dosage']})"
        if reminder.get('course_number', 1) > 1:
            text += f" [Курс #{reminder['course_number']}]"
        
        times_display = [t["time"] for t in reminder.get("times", [])]
        text += f"\n⏰ Времена: {', '.join(times_display) if times_display else 'не указано'}\n\n"
        text += f"📊 Статистика:\n"
        text += f"✅ Принято: {taken_count}\n"
        text += f"❌ Пропущено: {skipped_count}\n\n"
        text += "📝 Курс будет перенесен в архив с сохранением всей истории"

        keyboard = {
            "inline_keyboard": [
                [{"text": "✅ Да, завершить курс", "callback_data": f"confirm_archive_{reminder_id}"}],
                [{"text": "❌ Отмена", "callback_data": f"cancel_archive_{reminder_id}"}]
            ]
        }
        await self.edit_message_text(chat_id, message_id, text)
        await self.send_message(chat_id, text, keyboard)

    async def archive_reminder(self, chat_id, user_id, message_id, reminder_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if not user_data or reminder_id not in user_data.get("reminders", {}):
            await self.edit_message_text(chat_id, message_id, "❌ Напоминание не найдено")
            return

        reminder = user_data["reminders"][reminder_id]
        pill_name = reminder.get('pill_name', 'Витаминка')
        course_number = reminder.get('course_number', 1)

        # Получаем историю только для этого конкретного курса
        history_data = await self.storage.async_load() or {'history': []}
        course_history = [
            entry for entry in history_data.get('history', [])
            if (entry.get('user_id') == str(user_id) and
                entry.get('pill_name') == pill_name and
                entry.get('reminder_id') == reminder_id)
        ]

        # Подготавливаем архивную запись
        archive_data = await self.archive_storage.async_load() or {'archive': []}

        # Вычисляем даты начала и окончания
        start_date = None
        end_date = datetime.now().isoformat()
        if course_history:
            start_date = min(course_history, key=lambda x: x['date'])['date']
        elif reminder.get('created'):
            start_date = reminder['created']
        else:
            start_date = end_date

        taken_count = sum(1 for entry in course_history if entry['status'] == 'taken')
        skipped_count = sum(1 for entry in course_history if entry['status'] == 'skipped')

        archive_entry = {
            'user_id': str(user_id),
            'reminder_data': reminder.copy(),
            'history': course_history.copy(),
            'start_date': start_date,
            'end_date': end_date,
            'total_taken': taken_count,
            'total_skipped': skipped_count,
            'archived_at': datetime.now().isoformat()
        }

        archive_data['archive'].append(archive_entry)
        await self.archive_storage.async_save(archive_data)

        # Удаляем напоминание из активных
        del user_data["reminders"][reminder_id]
        await self.users_storage.async_save(users_data)

        # Убираем из активных напоминаний
        reminder_key = f"{user_id}_{reminder_id}"
        if reminder_key in self.active_reminders:
            del self.active_reminders[reminder_key]

        # Удаляем историю этого курса из основного хранилища
        updated_history = [
            entry for entry in history_data.get('history', [])
            if not (entry.get('user_id') == str(user_id) and
                   entry.get('pill_name') == pill_name and
                   entry.get('reminder_id') == reminder_id)
        ]
        history_data['history'] = updated_history
        await self.storage.async_save(history_data)

        text = f"✅ Курс завершен и перенесен в архив\n\n"
        text += f"💊 {pill_name}"
        if course_number > 1:
            text += f" (Курс #{course_number})"
        text += "\n\n"
        text += f"📊 Итоговая статистика:\n"
        text += f"✅ Принято: {taken_count}\n"
        text += f"❌ Пропущено: {skipped_count}\n\n"
        text += f"📅 Период: {datetime.fromisoformat(start_date).strftime('%d.%m.%Y')} - {datetime.fromisoformat(end_date).strftime('%d.%m.%Y')}\n\n"
        text += "🗄️ Используйте /archive для просмотра архива и повтора курсов"
        
        await self.edit_message_text(chat_id, message_id, text)

        # Обновляем сенсоры
        await self.update_sensors()

        # Возвращаемся к меню управления через 3 секунды, если есть другие напоминания
        await asyncio.sleep(3)
        if user_data.get("reminders"):
            await self.handle_manage_command(chat_id, user_id)
        else:
            await self.send_message(chat_id, "У вас больше нет активных напоминаний.\nИспользуйте /setup для создания нового.")

    async def save_reminder(self, chat_id, user_id, message_id, reminder_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if user_data and reminder_id in user_data.get("reminders", {}):
            user_data["reminders"][reminder_id]["active"] = True
            user_data.pop("setup_step", None)
            user_data.pop("current_reminder_id", None)
            await self.users_storage.async_save(users_data)

            reminder = user_data["reminders"][reminder_id]
            times_display = [t["time"] for t in reminder.get("times", [])]
            duration_text = f"{reminder.get('duration_days')} дней" if reminder.get('duration_days') else "бесконечно"

            text = "✅ Напоминание создано!\n\n"
            text += f"💊 Витаминка: {reminder['pill_name']}"
            if reminder.get('course_number', 1) > 1:
                text += f" (Курс #{reminder['course_number']})"
            text += "\n"
            
            if reminder.get('dosage'):
                text += f"📏 Дозировка: {reminder['dosage']}\n"
            if reminder.get('description'):
                text += f"💡 Описание: {reminder['description']}\n"
                
            text += f"📅 Длительность: {duration_text}\n"
            text += f"⏰ Времена приема: {', '.join(times_display)}\n\n"
            text += f"📢 Напоминания будут приходить в канал каждый день в указанные времена"
            
            await self.edit_message_text(chat_id, message_id, text)

            # Обновляем сенсоры
            await self.update_sensors()

    async def cancel_reminder(self, chat_id, user_id, message_id, reminder_id):
        users_data = await self.users_storage.async_load() or {}
        user_data = users_data.get(str(user_id))
        
        if user_data:
            # Удаляем незавершенное напоминание
            if reminder_id in user_data.get("reminders", {}):
                del user_data["reminders"][reminder_id]
            user_data.pop("setup_step", None)
            user_data.pop("current_reminder_id", None)
            await self.users_storage.async_save(users_data)

        text = "❌ Создание напоминания отменено\n\nИспользуйте /setup для создания нового напоминания"
        await self.edit_message_text(chat_id, message_id, text)

    async def reminder_scheduler(self):
        while True:
            try:
                await asyncio.sleep(60)  # Проверяем каждую минуту
                await self.check_and_send_reminders()
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error in reminder scheduler: %s", err)

    async def check_and_send_reminders(self):
        users_data = await self.users_storage.async_load() or {}
        current_time = datetime.now().strftime("%H:%M")
        
        for user_id, user_data in users_data.items():
            if not user_data.get("reminders"):
                continue
                
            for reminder_id, reminder in user_data["reminders"].items():
                if not reminder.get("active", True):
                    continue
                    
                # Проверяем все времена приема для этого напоминания
                for time_index, time_slot in enumerate(reminder.get("times", [])):
                    if time_slot.get("time") == current_time:
                        reminder_key = f"{user_id}_{reminder_id}_{time_index}"
                        if reminder_key not in self.active_reminders:
                            await self.send_user_reminder(user_id, user_data, reminder_id, reminder, time_index)

    async def send_user_reminder(self, user_id, user_data, reminder_id, reminder, time_index):
        try:
            reminder_key = f"{user_id}_{reminder_id}_{time_index}"
            time_slot = reminder["times"][time_index]
            
            self.active_reminders[reminder_key] = {
                'timestamp': datetime.now().isoformat(),
                'pill_name': reminder['pill_name'],
                'user_id': user_id,
                'reminder_id': reminder_id,
                'time_index': time_index
            }

            # Формируем сообщение с прогрессом курса
            username = user_data.get('username', user_data.get('first_name', 'Пользователь'))
            pill_display = reminder['pill_name']
            
            if reminder.get('dosage'):
                pill_display += f" ({reminder['dosage']})"
            if reminder.get('course_number', 1) > 1:
                pill_display += f" [Курс #{reminder['course_number']}]"

            # Вычисляем прогресс курса
            duration_days = reminder.get("duration_days")
            progress_text = ""
            if duration_days:
                start_date = datetime.fromisoformat(reminder.get('created', datetime.now().isoformat()))
                days_passed = (datetime.now() - start_date).days + 1
                days_left = max(0, duration_days - days_passed + 1)
                progress_text = f"\n📅 День {days_passed}/{duration_days} (осталось {days_left} дн.)"

            message = f"@{username} Время принять {pill_display}! 💊"
            message += f"\n⏰ Прием в {time_slot['time']}"
            message += progress_text

            keyboard = {
                "inline_keyboard": [
                    [{"text": "✅ Выпил", "callback_data": f"taken_{user_id}_{reminder_id}_{time_index}"}],
                    [{"text": "❌ Пропустить", "callback_data": f"skip_{user_id}_{reminder_id}_{time_index}"}],
                    [{"text": "📝 Описание", "callback_data": f"description_{user_id}_{reminder_id}"}]
                ]
            }

            await self.send_message(self.config[CONF_CHAT_ID], message, keyboard)
            self.hass.async_create_task(self.repeat_user_reminder(user_id, user_data, reminder_id, reminder, time_index))

        except Exception as err:
            _LOGGER.error("Error sending user reminder: %s", err)

    async def repeat_user_reminder(self, user_id, user_data, reminder_id, reminder, time_index):
        reminder_key = f"{user_id}_{reminder_id}_{time_index}"
        
        while reminder_key in self.active_reminders:
            try:
                await asyncio.sleep(1800)  # 30 минут
                if reminder_key in self.active_reminders:
                    time_slot = reminder["times"][time_index]
                    username = user_data.get('username', user_data.get('first_name', 'Пользователь'))
                    pill_display = reminder['pill_name']
                    
                    if reminder.get('dosage'):
                        pill_display += f" ({reminder['dosage']})"
                    if reminder.get('course_number', 1) > 1:
                        pill_display += f" [Курс #{reminder['course_number']}]"

                    message = f"⏰ @{username} Напоминание: не забудьте принять {pill_display}!"
                    message += f"\n⏰ Прием в {time_slot['time']}"

                    keyboard = {
                        "inline_keyboard": [
                            [{"text": "✅ Выпил", "callback_data": f"taken_{user_id}_{reminder_id}_{time_index}"}],
                            [{"text": "❌ Пропустить", "callback_data": f"skip_{user_id}_{reminder_id}_{time_index}"}],
                            [{"text": "📝 Описание", "callback_data": f"description_{user_id}_{reminder_id}"}]
                        ]
                    }
                    
                    await self.send_message(self.config[CONF_CHAT_ID], message, keyboard)
                    
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error in repeat user reminder: %s", err)

    async def mark_as_taken(self, chat_id, reminder_user_id, message_id, action_user_id, reminder_id="default", time_index=0):
        try:
            users_data = await self.users_storage.async_load() or {}
            user_data = users_data.get(str(reminder_user_id))
            action_user_data = users_data.get(str(action_user_id))
            
            if user_data:
                history_data = await self.storage.async_load() or {'history': []}
                
                # Находим конкретное напоминание или используем первое доступное
                pill_name = "витаминка"
                dosage = ""
                course_number = 1
                time_taken = "??:??"
                
                if reminder_id != "default" and reminder_id in user_data.get("reminders", {}):
                    reminder_info = user_data["reminders"][reminder_id]
                    pill_name = reminder_info.get("pill_name", "витаминка")
                    dosage = reminder_info.get("dosage", "")
                    course_number = reminder_info.get("course_number", 1)
                    times_list = reminder_info.get("times", [])
                    if time_index < len(times_list):
                        time_taken = times_list[time_index].get("time", "??:??")
                elif user_data.get("reminders"):
                    first_reminder = next(iter(user_data["reminders"].values()))
                    pill_name = first_reminder.get("pill_name", "витаминка")
                    dosage = first_reminder.get("dosage", "")
                    course_number = first_reminder.get("course_number", 1)
                    times_list = first_reminder.get("times", [])
                    if times_list:
                        time_taken = times_list[0].get("time", "??:??")

                entry = {
                    'date': datetime.now().isoformat(),
                    'status': 'taken',
                    'user_id': reminder_user_id,
                    'reminder_id': reminder_id,
                    'pill_name': pill_name,
                    'dosage': dosage,
                    'course_number': course_number,
                    'time_index': time_index,
                    'time_taken': time_taken,
                    'action_by': action_user_id
                }
                
                history_data['history'].append(entry)
                await self.storage.async_save(history_data)

                # Убираем активное напоминание
                reminder_key = f"{reminder_user_id}_{reminder_id}_{time_index}"
                if reminder_key in self.active_reminders:
                    del self.active_reminders[reminder_key]

                # Уведомляем пользователя в личные сообщения
                if user_data.get('chat_id'):
                    pill_display = pill_name
                    if dosage:
                        pill_display += f" ({dosage})"
                    if course_number > 1:
                        pill_display += f" [Курс #{course_number}]"
                    
                    personal_msg = f"✅ Отлично! {pill_display} принята в {time_taken}!"
                    if str(action_user_id) != str(reminder_user_id):
                        action_username = action_user_data.get('username', 'кто-то') if action_user_data else 'кто-то'
                        personal_msg += f"\n(Отмечено пользователем @{action_username})"
                    
                    await self.send_message(user_data['chat_id'], personal_msg)

                # Обновляем сообщение в канале
                pill_display = pill_name
                if dosage:
                    pill_display += f" ({dosage})"
                if course_number > 1:
                    pill_display += f" [Курс #{course_number}]"
                    
                channel_msg = f"✅ {pill_display} принята в {time_taken}!"
                await self.edit_message_text(chat_id, message_id, channel_msg)

                # Обновляем сенсоры
                _LOGGER.info(f"Pill taken: {pill_name} (course #{course_number}) at {time_taken} by user {reminder_user_id}")
                await self.update_sensors()

        except Exception as err:
            _LOGGER.error("Error marking as taken: %s", err)

    async def mark_as_skipped(self, chat_id, reminder_user_id, message_id, action_user_id, reminder_id="default", time_index=0):
        try:
            users_data = await self.users_storage.async_load() or {}
            user_data = users_data.get(str(reminder_user_id))
            action_user_data = users_data.get(str(action_user_id))
            
            if user_data:
                history_data = await self.storage.async_load() or {'history': []}
                
                # Находим конкретное напоминание или используем первое доступное
                pill_name = "витаминка"
                dosage = ""
                course_number = 1
                time_skipped = "??:??"
                
                if reminder_id != "default" and reminder_id in user_data.get("reminders", {}):
                    reminder_info = user_data["reminders"][reminder_id]
                    pill_name = reminder_info.get("pill_name", "витаминка")
                    dosage = reminder_info.get("dosage", "")
                    course_number = reminder_info.get("course_number", 1)
                    times_list = reminder_info.get("times", [])
                    if time_index < len(times_list):
                        time_skipped = times_list[time_index].get("time", "??:??")
                elif user_data.get("reminders"):
                    first_reminder = next(iter(user_data["reminders"].values()))
                    pill_name = first_reminder.get("pill_name", "витаминка")
                    dosage = first_reminder.get("dosage", "")
                    course_number = first_reminder.get("course_number", 1)
                    times_list = first_reminder.get("times", [])
                    if times_list:
                        time_skipped = times_list[0].get("time", "??:??")

                entry = {
                    'date': datetime.now().isoformat(),
                    'status': 'skipped',
                    'user_id': reminder_user_id,
                    'reminder_id': reminder_id,
                    'pill_name': pill_name,
                    'dosage': dosage,
                    'course_number': course_number,
                    'time_index': time_index,
                    'time_taken': time_skipped,
                    'action_by': action_user_id
                }
                
                history_data['history'].append(entry)
                await self.storage.async_save(history_data)

                # Убираем активное напоминание
                reminder_key = f"{reminder_user_id}_{reminder_id}_{time_index}"
                if reminder_key in self.active_reminders:
                    del self.active_reminders[reminder_key]

                # Уведомляем пользователя в личные сообщения
                if user_data.get('chat_id'):
                    pill_display = pill_name
                    if dosage:
                        pill_display += f" ({dosage})"
                    if course_number > 1:
                        pill_display += f" [Курс #{course_number}]"
                    
                    personal_msg = f"❌ {pill_display} пропущена в {time_skipped}. Записано в историю."
                    if str(action_user_id) != str(reminder_user_id):
                        action_username = action_user_data.get('username', 'кто-то') if action_user_data else 'кто-то'
                        personal_msg += f"\n(Отмечено пользователем @{action_username})"
                    
                    await self.send_message(user_data['chat_id'], personal_msg)

                # Обновляем сообщение в канале
                pill_display = pill_name
                if dosage:
                    pill_display += f" ({dosage})"
                if course_number > 1:
                    pill_display += f" [Курс #{course_number}]"
                    
                channel_msg = f"❌ {pill_display} пропущена в {time_skipped}"
                await self.edit_message_text(chat_id, message_id, channel_msg)

                # Обновляем сенсоры
                _LOGGER.info(f"Pill skipped: {pill_name} (course #{course_number}) at {time_skipped} by user {reminder_user_id}")
                await self.update_sensors()

        except Exception as err:
            _LOGGER.error("Error marking as skipped: %s", err)

    async def get_user_history(self, user_id, active_only=False):
        try:
            history_data = await self.storage.async_load() or {'history': []}
            users_data = await self.users_storage.async_load() or {}
            user_data = users_data.get(str(user_id))
            
            week_ago = datetime.now() - timedelta(days=7)
            user_history = [
                entry for entry in history_data.get('history', [])
                if (entry.get('user_id') == str(user_id) and
                    datetime.fromisoformat(entry['date']) >= week_ago)
            ]

            # Если нужна только история активных витаминок
            if active_only and user_data and user_data.get("reminders"):
                active_pills = set()
                for reminder in user_data["reminders"].values():
                    active_pills.add(reminder.get("pill_name"))
                user_history = [
                    entry for entry in user_history
                    if entry.get('pill_name') in active_pills
                ]

            # Группируем по витаминкам и курсам
            pills_stats = {}
            for entry in user_history:
                pill_name = entry.get('pill_name', 'Неизвестно')
                course_number = entry.get('course_number', 1)
                dosage = entry.get('dosage', '')
                
                pill_key = pill_name
                if dosage:
                    pill_key += f" ({dosage})"
                if course_number > 1:
                    pill_key += f" [Курс #{course_number}]"
                
                if pill_key not in pills_stats:
                    pills_stats[pill_key] = {'taken': 0, 'skipped': 0}
                pills_stats[pill_key][entry['status']] += 1

            username = user_data.get('username', user_data.get('first_name', 'Пользователь')) if user_data else 'Пользователь'
            
            if active_only:
                history_text = f"📊 История активных витаминок {username} за неделю:\n\n"
            else:
                history_text = f"📊 История {username} за неделю:\n\n"

            if pills_stats:
                for pill_key, stats in pills_stats.items():
                    history_text += f"💊 {pill_key}:\n"
                    history_text += f"   ✅ Принято: {stats['taken']}\n"
                    history_text += f"   ❌ Пропущено: {stats['skipped']}\n\n"

                history_text += "📋 Последние записи:\n"
                # Показываем последние 7 записей
                recent_entries = sorted(user_history, key=lambda x: x['date'], reverse=True)[:7]
                for entry in recent_entries:
                    date = datetime.fromisoformat(entry['date']).strftime("%d.%m %H:%M")
                    status = "✅" if entry['status'] == 'taken' else "❌"
                    pill_display = entry['pill_name']
                    
                    if entry.get('dosage'):
                        pill_display += f" ({entry['dosage']})"
                    if entry.get('course_number', 1) > 1:
                        pill_display += f" [Курс #{entry['course_number']}]"
                    
                    time_info = ""
                    if entry.get('time_taken'):
                        time_info = f" в {entry['time_taken']}"
                    
                    history_text += f"{status} {date} - {pill_display}{time_info}\n"
            else:
                if active_only:
                    history_text += "Нет записей по активным витаминкам за последнюю неделю"
                else:
                    history_text += "Нет записей за последнюю неделю"

            if active_only:
                history_text += "\n\n🗄️ Используйте /archive для просмотра завершенных курсов"

            return history_text

        except Exception as err:
            _LOGGER.error("Error getting user history: %s", err)
            return "Ошибка при получении истории"

    async def get_user_archive(self, user_id):
        try:
            archive_data = await self.archive_storage.async_load() or {'archive': []}
            user_archive = [
                entry for entry in archive_data.get('archive', [])
                if entry.get('user_id') == str(user_id)
            ]

            users_data = await self.users_storage.async_load() or {}
            user_data = users_data.get(str(user_id))
            username = user_data.get('username', user_data.get('first_name', 'Пользователь')) if user_data else 'Пользователь'

            archive_text = f"🗄️ Архив завершенных курсов {username}:\n\n"

            if user_archive:
                # Сортируем по дате завершения (новые сверху)
                sorted_archive = sorted(user_archive, key=lambda x: x['end_date'], reverse=True)
                
                for entry in sorted_archive:
                    reminder_data = entry.get('reminder_data', {})
                    pill_name = reminder_data.get('pill_name', 'Неизвестно')
                    dosage = reminder_data.get('dosage', '')
                    description = reminder_data.get('description', '')
                    course_number = reminder_data.get('course_number', 1)
                    duration_days = reminder_data.get('duration_days')
                    
                    start_date = datetime.fromisoformat(entry['start_date']).strftime('%d.%m.%Y')
                    end_date = datetime.fromisoformat(entry['end_date']).strftime('%d.%m.%Y')

                    archive_text += f"💊 {pill_name}"
                    if dosage:
                        archive_text += f" ({dosage})"
                    if course_number > 1:
                        archive_text += f" [Курс #{course_number}]"
                    archive_text += "\n"
                    
                    if description:
                        archive_text += f"   💡 {description}\n"
                        
                    archive_text += f"   📅 {start_date} - {end_date}"
                    if duration_days:
                        archive_text += f" ({duration_days} дн.)"
                    archive_text += "\n"
                    
                    times_display = [t["time"] for t in reminder_data.get("times", [])]
                    if times_display:
                        archive_text += f"   ⏰ Времена: {', '.join(times_display)}\n"
                    
                    archive_text += f"   ✅ Принято: {entry.get('total_taken', 0)}\n"
                    archive_text += f"   ❌ Пропущено: {entry.get('total_skipped', 0)}\n"
                    
                    # Вычисляем процент соблюдения
                    total = entry.get('total_taken', 0) + entry.get('total_skipped', 0)
                    if total > 0:
                        compliance = round(entry.get('total_taken', 0) / total * 100, 1)
                        archive_text += f"   📊 Соблюдение: {compliance}%\n"
                    archive_text += "\n"

                archive_text += f"📋 Всего завершенных курсов: {len(sorted_archive)}\n\n"
                archive_text += "🔄 Используйте кнопки ниже для повтора родительских курсов (Курс #1)"
            else:
                archive_text += "Архив пуст\n\n"
                archive_text += "Завершенные курсы будут отображаться здесь"

            return archive_text

        except Exception as err:
            _LOGGER.error("Error getting user archive: %s", err)
            return "Ошибка при получении архива"

    async def update_sensors(self):
        """Update Home Assistant sensors."""
        try:
            # Находим координатор для этой интеграции
            domain_data = self.hass.data.get(DOMAIN, {})
            for entry_id, entry_data in domain_data.items():
                if isinstance(entry_data, dict) and 'coordinator' in entry_data:
                    coordinator = entry_data['coordinator']
                    if coordinator:
                        await coordinator.async_request_refresh()
                        _LOGGER.debug("Sensors updated successfully")
                        break
            else:
                _LOGGER.warning("Coordinator not found for sensor update")
        except Exception as err:
            _LOGGER.error("Error updating sensors: %s", err)

    async def send_message(self, chat_id, text, reply_markup=None):
        url = f"{self.base_url}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        if reply_markup:
            data["reply_markup"] = reply_markup
        async with self.session.post(url, json=data) as response:
            return await response.json()

    async def edit_message_text(self, chat_id, message_id, text):
        url = f"{self.base_url}/editMessageText"
        data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML"
        }
        async with self.session.post(url, json=data) as response:
            return await response.json()

    async def answer_callback_query(self, callback_query_id):
        url = f"{self.base_url}/answerCallbackQuery"
        data = {"callback_query_id": callback_query_id}
        async with self.session.post(url, json=data) as response:
            return await response.json()