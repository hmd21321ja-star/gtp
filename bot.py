import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import requests
import urllib.parse

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def get_response(user_message):
    try:
        encoded_message = urllib.parse.quote(user_message)
        url = f"https://ai-chat.apisimpacientes.workers.dev/chat?model=wormgpt&prompt={encoded_message}"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'response' in data:
                return data['response']
            else:
                return "عذراً، لم أتمكن من فهم الرد"
        else:
            return f"حدث خطأ: {response.status_code}"
            
    except requests.exceptions.Timeout:
        return "انتهت المهلة، حاول مرة أخرى"
    except:
        return "حدث خطأ في الاتصال"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(f"أهلاً {user.first_name}! أنا بوت WormGPT. تحدث معي في أي موضوع تريده.")

async def chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text
    
    if not user_message.strip():
        await update.message.reply_text("اكتب شيئاً لأرد عليك")
        return
    
    await update.message.reply_chat_action(action="typing")
    
    bot_response = get_response(user_message)
    
    await update.message.reply_text(bot_response)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("أرسل لي أي رسالة وسأرد عليك. جرب أن تسألني عن أي شيء!")

def main():
    TOKEN = "8887221645:AAG-5QqkkZElre44JBWUBwBpL8Jp9z0Kj9s"
    
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, chat))
    
    print("البوت يعمل الآن...")
    app.run_polling()

if __name__ == '__main__':
    main()
