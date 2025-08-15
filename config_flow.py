import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult
import homeassistant.helpers.config_validation as cv
from .const import *

class PillsReminderConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None) -> FlowResult:
        errors = {}
        
        if user_input is not None:
            return self.async_create_entry(
                title=f"Pills Reminder Bot",
                data=user_input
            )

        data_schema = vol.Schema({
            vol.Required(CONF_BOT_TOKEN): str,
            vol.Required(CONF_CHAT_ID): str,
        })

        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "setup_instructions": "Создайте бота через @BotFather и получите токен. Добавьте бота в группу и получите Chat ID."
            }
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return PillsReminderOptionsFlow(config_entry)

class PillsReminderOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, config_entry):
        self.config_entry = config_entry

    async def async_step_init(self, user_input=None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        data_schema = vol.Schema({
            vol.Optional(CONF_BOT_TOKEN, default=self.config_entry.data.get(CONF_BOT_TOKEN, "")): str,
            vol.Optional(CONF_CHAT_ID, default=self.config_entry.data.get(CONF_CHAT_ID, "")): str,
        })

        return self.async_show_form(
            step_id="init",
            data_schema=data_schema
        )