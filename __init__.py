import logging
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from .const import DOMAIN
from .telegram_bot import PillsReminderBot

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})
    
    try:
        bot = PillsReminderBot(hass, {**entry.data, **entry.options})
        
        # Сначала сохраняем бота
        hass.data[DOMAIN][entry.entry_id] = {
            'bot': bot,
            'sensors': {},
            'coordinator': None  # Будет заполнено из sensor.py
        }
        
        # Настройка платформы сенсоров
        await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])
        
        # Запускаем бота после настройки сенсоров
        await bot.start()
        
        entry.async_on_unload(entry.add_update_listener(update_listener))
        
        return True
    except Exception as err:
        _LOGGER.error("Failed to setup pills reminder bot: %s", err)
        return False

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    await hass.config_entries.async_forward_entry_unload(entry, "sensor")
    
    data = hass.data[DOMAIN].pop(entry.entry_id)
    if 'bot' in data:
        await data['bot'].stop()
    return True

async def update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)