import asyncio, os, re, subprocess, uuid, shutil, zipfile, psutil, json, time, resource, signal, threading
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ==================== الإعدادات ====================
BOT_TOKEN = "8887221645:AAG-5QqkkZElre44JBWUBwBpL8Jp9z0Kj9s"
OWNER_ID = 47230981
PUBLIC_MODE = True

BASE_DIR = "/root/bot_runs"
os.makedirs(BASE_DIR, exist_ok=True)
STATS_FILE = os.path.join(BASE_DIR, "user_stats.json")
ENVS_FILE = os.path.join(BASE_DIR, "user_envs.json")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUTS_DIR, exist_ok=True)

MENU_VIDEO_URL = "https://stream.vidhosting.in/videos/5d4d3287.mp4"

running_processes = {}   
pending_uploads = {}     
uploaded_files = {}      
user_states = {}
user_stats = {}          
auto_restart_watchers = {}  
debug_modes = {}         
process_outputs = {}     
process_streams = {}     

#///////////////#///////////////////////////////////////////////
def load_stats():
    global user_stats
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, 'r') as f:
                user_stats = json.load(f)
        except:
            user_stats = {}
    else:
        user_stats = {}

def save_stats():
    with open(STATS_FILE, 'w') as f:
        json.dump(user_stats, f, indent=2)

def update_stats(user_id, success=True, error_msg=None, runtime=0):
    uid = str(user_id)
    if uid not in user_stats:
        user_stats[uid] = {"runs": 0, "total_time": 0, "errors": 0, "libraries": [], "last_run": ""}
    user_stats[uid]["runs"] += 1
    user_stats[uid]["total_time"] += runtime
    user_stats[uid]["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not success or error_msg:
        user_stats[uid]["errors"] += 1
    save_stats()

def load_envs():
    if os.path.exists(ENVS_FILE):
        try:
            with open(ENVS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_envs(envs):
    with open(ENVS_FILE, 'w') as f:
        json.dump(envs, f, indent=2)

def get_user_stats_text(user_id):
    uid = str(user_id)
    stats = user_stats.get(uid, {})
    if not stats:
        return "لا توجد إحصائيات لهذا المستخدم."
    text = (
        f"إحصائيات المستخدم\n"
        f"عدد مرات التشغيل: {stats.get('runs', 0)}\n"
        f"إجمالي وقت التشغيل: {stats.get('total_time', 0):.2f} ثانية\n"
        f"عدد الأخطاء: {stats.get('errors', 0)}\n"
        f"آخر تشغيل: {stats.get('last_run', 'غير معروف')}\n"
        f"المكتبات المثبتة: {', '.join(stats.get('libraries', [])) or 'لا توجد'}"
    )
    return text

# ==================== دوال الحماية المتقدمة (Sandbox) ====================
def set_resource_limits(timeout_seconds=60, memory_mb=512):
    try:
        resource.setrlimit(resource.RLIMIT_CPU, (timeout_seconds, timeout_seconds + 10))
        memory_limit = memory_mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
        resource.setrlimit(resource.RLIMIT_NOFILE, (100, 100))
        return True
    except Exception as e:
        print(f"فشل تعيين حدود الموارد: {e}")
        return False

async def run_with_timeout(cmd, cwd, env, timeout_seconds=120):
    """تشغيل أمر مع حد زمني."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
            return out, err, proc.returncode
        except asyncio.TimeoutError:
            try:
                proc.terminate()
                await asyncio.sleep(1)
                if proc.returncode is None:
                    proc.kill()
            except:
                pass
            return b"", b"", -1
    except Exception as e:
        return b"", str(e).encode(), -1

# ==================== دوال إرسال المخرجات بشكل فوري (Streaming) ====================
async def stream_output(proc, user_id, context, folder):
    """قراءة stdout و stderr بشكل فوري وإرسال المخرجات للمستخدم."""
    output_lines = []
    error_lines = []
    output_pending = []
    error_pending = []

    async def read_stream(stream, all_lines, pending_lines, label):
        try:
            while True:
                # StreamReader.readline() دالة async، لذلك يجب استخدام await مباشرة.
                line = await stream.readline()
                if not line:
                    break

                # asyncio subprocess يعيد bytes افتراضياً، لذلك نحولها إلى نص.
                if isinstance(line, bytes):
                    line = line.decode("utf-8", errors="replace")

                line = line.strip()
                if not line:
                    continue

                all_lines.append(line)
                pending_lines.append(line)

                if len(pending_lines) >= 5:
                    text = f"{label}:\n" + "\n".join(pending_lines[-5:])
                    await context.bot.send_message(user_id, text[:4000])
                    pending_lines.clear()

            # إرسال أي مخرجات متبقية بعد انتهاء القراءة.
            if pending_lines:
                text = f"{label}:\n" + "\n".join(pending_lines[-5:])
                await context.bot.send_message(user_id, text[:4000])
                pending_lines.clear()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            await context.bot.send_message(user_id, f"خطأ في قراءة {label}: {e}")

    try:
        # قراءة stdout و stderr بالتوازي حتى لا يمتلئ أحد الـ pipes ويتوقف البرنامج.
        stdout_task = asyncio.create_task(
            read_stream(proc.stdout, output_lines, output_pending, "مخرجات")
        )
        stderr_task = asyncio.create_task(
            read_stream(proc.stderr, error_lines, error_pending, "خطأ")
        )

        await asyncio.gather(stdout_task, stderr_task)

        # التأكد من انتهاء العملية قبل فحص returncode في monitor().
        await proc.wait()

        return output_lines, error_lines

    except asyncio.CancelledError:
        raise
    except Exception as e:
        await context.bot.send_message(user_id, f"خطأ في قراءة المخرجات: {e}")
        return output_lines, error_lines

# ==================== دوال إعادة التشغيل التلقائي ====================
class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, folder, user_id, context):
        self.folder = folder
        self.user_id = user_id
        self.context = context
        self.last_restart = time.time()
    
    def on_modified(self, event):
        if event.is_file and event.src_path.endswith('.py'):
            if time.time() - self.last_restart > 5:
                self.last_restart = time.time()
                asyncio.run_coroutine_threadsafe(
                    self.restart_file(),
                    self.context.application.loop
                )
    
    async def restart_file(self):
        folder = self.folder
        info = uploaded_files.get(folder)
        if not info:
            return
        user_id = info["user_id"]
        path = info["path"]
        await self.context.bot.send_message(user_id, "تم اكتشاف تعديل على الملف، جاري إعادة التشغيل...")
        await stop_process_by_user(user_id)
        await handle_uploaded_file(user_id, folder, path, self.context, restart=True)

# ==================== دوال تثبيت المكتبات بشكل ذكي ====================
async def smart_install_libraries(work_dir, user_id, context):
    """تثبيت المكتبات بشكل ذكي باستخدام pipenv أو poetry أو pip."""
    requirements = []
    
    req_path = os.path.join(work_dir, "requirements.txt")
    if os.path.exists(req_path):
        with open(req_path, 'r') as f:
            requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    pipfile_path = os.path.join(work_dir, "Pipfile")
    pyproject_path = os.path.join(work_dir, "pyproject.toml")
    
    if os.path.exists(pipfile_path):
        proc = await asyncio.create_subprocess_exec(
            "pipenv", "install",
            cwd=work_dir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, err = await proc.communicate()
        if proc.returncode == 0:
            await context.bot.send_message(user_id, "تم تثبيت المكتبات باستخدام Pipenv.")
            # تحديث الإحصائيات
            uid = str(user_id)
            if uid in user_stats:
                user_stats[uid]["libraries"] = list(set(user_stats[uid]["libraries"] + ["pipenv"]))
                save_stats()
            return True
        else:
            await context.bot.send_message(user_id, f"فشل Pipenv، جاري استخدام pip:\n{err.decode()[:500]}")
    
    if os.path.exists(pyproject_path):
        proc = await asyncio.create_subprocess_exec(
            "poetry", "install",
            cwd=work_dir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, err = await proc.communicate()
        if proc.returncode == 0:
            await context.bot.send_message(user_id, "تم تثبيت المكتبات باستخدام Poetry.")
            uid = str(user_id)
            if uid in user_stats:
                user_stats[uid]["libraries"] = list(set(user_stats[uid]["libraries"] + ["poetry"]))
                save_stats()
            return True
        else:
            await context.bot.send_message(user_id, f"فشل Poetry، جاري استخدام pip:\n{err.decode()[:500]}")
    
    if requirements:
        proc = await asyncio.create_subprocess_exec(
            "python3", "-m", "pip", "install", *requirements,
            cwd=work_dir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        out, err = await proc.communicate()
        if proc.returncode == 0:
            await context.bot.send_message(user_id, f"تم تثبيت {len(requirements)} مكتبة بنجاح.")
            uid = str(user_id)
            if uid in user_stats:
                user_stats[uid]["libraries"] = list(set(user_stats[uid]["libraries"] + requirements))
                save_stats()
            return True
        else:
            await context.bot.send_message(user_id, f"فشل تثبيت بعض المكتبات:\n{err.decode()[:500]}")
            return False
    
    return True

# ==================== دوال البوت الأساسية ====================
def is_authorized(user_id):
    if PUBLIC_MODE:
        return True
    return user_id == OWNER_ID

async def stop_process_by_user(user_id):
    info = running_processes.pop(user_id, None)
    if info:
        try:
            proc = info["proc"]
            proc.terminate()
            await asyncio.wait_for(proc.wait(), timeout=5)
        except:
            try:
                proc.kill()
                await proc.wait()
            except:
                pass
        # إيقاف تدفق المخرجات
        if user_id in process_streams:
            process_streams[user_id]["stop"] = True
        return True
    return False

async def stop_process_by_folder(folder):
    for uid, info in list(running_processes.items()):
        if info.get("folder") == folder:
            try:
                proc = info["proc"]
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except:
                try:
                    proc.kill()
                    await proc.wait()
                except:
                    pass
            running_processes.pop(uid, None)
            if uid in process_streams:
                process_streams[uid]["stop"] = True
            return True
    return False

def main_menu_keyboard(user_id):
    buttons = [
        [
            InlineKeyboardButton("رفع ملف", callback_data="upload", style="success"),
            InlineKeyboardButton("إيقاف التشغيل", callback_data="stop", style="danger")
        ],
        [
            InlineKeyboardButton("تثبيت مكتبة", callback_data="install_library", style="primary"),
            InlineKeyboardButton("إحصائيات", callback_data="stats", style="primary")
        ],
        [
            InlineKeyboardButton("إحصائيات متقدمة", callback_data="advanced_stats", style="primary"),
            InlineKeyboardButton("تعيين متغيرات البيئة", callback_data="set_env", style="primary")
        ],
    ]
    if user_id == OWNER_ID:
        buttons.append([InlineKeyboardButton("إدارة الملفات", callback_data="manage_files", style="primary")])
    buttons.append([
        InlineKeyboardButton("المطور 1", url="https://t.me/@g_m_r6", style="primary"),
        InlineKeyboardButton("المطور 2", url="https://t.me/oounun", style="primary")
    ])
    return InlineKeyboardMarkup(buttons)

CAPTION_TEXT = (
    "مرحبا بك في بوت تشغيل ملفات بايثون .\n\n"
    "المميزات:\n"
    "- رفع وتشغيل ملفات .py أو مشاريع .zip مع تثبيت المكتبات تلقائياً.\n"
    "\n"
    "- إحصائيات السيرفر لحظة بلحظة.\n"
    "- إحصائيات متقدمة لكل مستخدم.\n"
    "\n"
    "\n"
    "- إعادة التشغيل التلقائي عند التعديل.\n"
    "- عرض المخرجات بشكل فوري.\n"
    "\n"
    "\n"
    "- إعادة تشغيل أي مشروع بنقرة زر.\n\n"
    "كل مستخدم له مساحة معزولة تمامًا.\n"
    "البوت سريع جدا وقوي وآمن\n\n"
    "اختر أحد الخيارات أدناه للبدء."
)

async def safe_edit(query, text=None, caption=None, reply_markup=None):
    msg = query.message
    try:
        if msg.video or msg.photo:
            await query.edit_message_caption(caption=caption or text, reply_markup=reply_markup)
        else:
            await query.edit_message_text(text=text or caption, reply_markup=reply_markup)
    except Exception:
        try:
            await msg.delete()
        except:
            pass
        if msg.video or msg.photo:
            await msg.chat.send_video(MENU_VIDEO_URL, caption=CAPTION_TEXT, reply_markup=reply_markup or main_menu_keyboard(msg.chat_id))
        else:
            await msg.chat.send_message(text=text or caption or CAPTION_TEXT, reply_markup=reply_markup or main_menu_keyboard(msg.chat_id))

async def send_main_menu(update_or_query, context):
    chat_id = update_or_query.message.chat_id if hasattr(update_or_query, 'message') else update_or_query.message.chat_id
    user_id = update_or_query.effective_user.id if hasattr(update_or_query, 'effective_user') else update_or_query.from_user.id
    await context.bot.send_video(chat_id, MENU_VIDEO_URL, caption=CAPTION_TEXT, reply_markup=main_menu_keyboard(user_id))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("غير مصرح")
        return
    load_stats()
    await send_main_menu(update, context)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    uid = q.from_user.id

    if not is_authorized(uid):
        await q.answer("غير مصرح", show_alert=True)
        return

    # رفع ملف
    if data == "upload":
        user_states[uid] = "awaiting_file"
        await safe_edit(q, text="أرسل ملف بايثون (.py) أو مشروع مضغوط (.zip). يمكنك أيضاً إرسال رابط تحميل مباشر لملف كبير.")

    # إيقاف التشغيل
    elif data == "stop":
        if uid not in running_processes:
            await safe_edit(q, text="لا توجد عملية قيد التشغيل خاصة بك.", reply_markup=main_menu_keyboard(uid))
        else:
            await stop_process_by_user(uid)
            await safe_edit(q, text="تم إيقاف العملية الخاصة بك.", reply_markup=main_menu_keyboard(uid))

    # تثبيت مكتبة
    elif data == "install_library":
        user_states[uid] = "awaiting_library"
        await safe_edit(q, text="أرسل اسم المكتبة التي تريد تثبيتها:")

    # إحصائيات السيرفر
    elif data == "stats":
        cpu_percent = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        stats_text = (
            f"إحصائيات السيرفر\n"
            f"المعالج: {cpu_percent}%\n"
            f"الذاكرة: {mem.used / (1024**3):.2f}GB / {mem.total / (1024**3):.2f}GB ({mem.percent}%)\n"
            f"المساحة: {disk.used / (1024**3):.2f}GB / {disk.total / (1024**3):.2f}GB ({disk.percent}%)\n"
            f"وقت التشغيل: {round(psutil.boot_time())} ثانية منذ الإقلاع"
        )
        await safe_edit(q, text=stats_text, reply_markup=main_menu_keyboard(uid))

    # إحصائيات متقدمة للمستخدم
    elif data == "advanced_stats":
        stats_text = get_user_stats_text(uid)
        await safe_edit(q, text=stats_text, reply_markup=main_menu_keyboard(uid))

    # تعيين متغيرات البيئة
    elif data == "set_env":
        user_states[uid] = "awaiting_env"
        await safe_edit(q, text="أرسل متغيرات البيئة بصيغة KEY=VALUE، كل متغير في سطر منفصل:\nمثال:\nAPI_KEY=123456\nDATABASE_URL=postgresql://user:pass@localhost/db")

    # إدارة الملفات (للمالك فقط)
    elif data == "manage_files":
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        if not uploaded_files:
            await safe_edit(q, text="لا توجد ملفات مرفوعة حتى الآن.", reply_markup=main_menu_keyboard(uid))
            return
        text = "الملفات المرفوعة والمشغلة:\n\n"
        keyboard_buttons = []
        for folder, info in uploaded_files.items():
            filename = info.get("filename", "غير معروف")
            user_name = info.get("owner_name", str(info["user_id"]))
            is_running = any(info2.get("folder") == folder for info2 in running_processes.values())
            status = "قيد التشغيل" if is_running else "متوقف"
            text += f"المستخدم: {user_name}\nالملف: {filename}\nالحالة: {status}\nالمجلد: {folder}\n\n"
            keyboard_buttons.append([InlineKeyboardButton(f"إيقاف {filename}", callback_data=f"stop_file:{folder}")])
            keyboard_buttons.append([InlineKeyboardButton(f"إعادة تشغيل {filename}", callback_data=f"restart_file:{folder}")])
            if is_running and folder in auto_restart_watchers:
                keyboard_buttons.append([InlineKeyboardButton(f"إيقاف المراقبة {filename}", callback_data=f"stop_watch:{folder}")])
            else:
                keyboard_buttons.append([InlineKeyboardButton(f"مراقبة التغييرات {filename}", callback_data=f"watch_file:{folder}")])
            # زر تفعيل وضع التصحيح
            if debug_modes.get(info["user_id"], False):
                keyboard_buttons.append([InlineKeyboardButton(f"إيقاف التصحيح {filename}", callback_data=f"debug_off:{folder}")])
            else:
                keyboard_buttons.append([InlineKeyboardButton(f"تفعيل التصحيح {filename}", callback_data=f"debug_on:{folder}")])
        keyboard_buttons.append([InlineKeyboardButton("الرجوع", callback_data="back_main")])
        reply_markup = InlineKeyboardMarkup(keyboard_buttons)
        await safe_edit(q, text=text, reply_markup=reply_markup)

    # إيقاف ملف معين
    elif data.startswith("stop_file:"):
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        folder = data.split(":", 1)[1]
        if await stop_process_by_folder(folder):
            await safe_edit(q, text=f"تم إيقاف الملف في المجلد {folder}.", reply_markup=main_menu_keyboard(uid))
        else:
            await safe_edit(q, text="الملف غير موجود أو لم يكن قيد التشغيل.", reply_markup=main_menu_keyboard(uid))

    # إعادة تشغيل ملف
    elif data.startswith("restart_file:"):
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        folder = data.split(":", 1)[1]
        info = uploaded_files.get(folder)
        if not info:
            await safe_edit(q, text="الملف غير موجود.", reply_markup=main_menu_keyboard(uid))
            return
        await stop_process_by_folder(folder)
        await safe_edit(q, text=f"جاري إعادة تشغيل الملف...")
        await handle_uploaded_file(info["user_id"], folder, info["path"], context, restart=True)

    # تفعيل وضع التصحيح
    elif data.startswith("debug_on:"):
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        folder = data.split(":", 1)[1]
        info = uploaded_files.get(folder)
        if not info:
            await safe_edit(q, text="الملف غير موجود.", reply_markup=main_menu_keyboard(uid))
            return
        debug_modes[info["user_id"]] = True
        await safe_edit(q, text=f"تم تفعيل وضع التصحيح للملف {info.get('filename', 'غير معروف')}. سيتم عرض مخرجات تفصيلية.", reply_markup=main_menu_keyboard(uid))

    # إيقاف وضع التصحيح
    elif data.startswith("debug_off:"):
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        folder = data.split(":", 1)[1]
        info = uploaded_files.get(folder)
        if not info:
            await safe_edit(q, text="الملف غير موجود.", reply_markup=main_menu_keyboard(uid))
            return
        debug_modes[info["user_id"]] = False
        await safe_edit(q, text=f"تم إيقاف وضع التصحيح للملف {info.get('filename', 'غير معروف')}.", reply_markup=main_menu_keyboard(uid))

    # تفعيل مراقبة التغييرات
    elif data.startswith("watch_file:"):
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        folder = data.split(":", 1)[1]
        info = uploaded_files.get(folder)
        if not info:
            await safe_edit(q, text="الملف غير موجود.", reply_markup=main_menu_keyboard(uid))
            return
        work_dir = os.path.join(BASE_DIR, folder)
        event_handler = FileChangeHandler(folder, info["user_id"], context)
        observer = Observer()
        observer.schedule(event_handler, work_dir, recursive=True)
        observer.start()
        auto_restart_watchers[folder] = observer
        await safe_edit(q, text=f"تم تفعيل مراقبة التغييرات للملف {info.get('filename', 'غير معروف')}. سيتم إعادة التشغيل تلقائياً عند التعديل.", reply_markup=main_menu_keyboard(uid))

    # إيقاف مراقبة التغييرات
    elif data.startswith("stop_watch:"):
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        folder = data.split(":", 1)[1]
        if folder in auto_restart_watchers:
            auto_restart_watchers[folder].stop()
            auto_restart_watchers[folder].join()
            del auto_restart_watchers[folder]
            await safe_edit(q, text=f"تم إيقاف مراقبة التغييرات للمجلد {folder}.", reply_markup=main_menu_keyboard(uid))
        else:
            await safe_edit(q, text="لا توجد مراقبة نشطة لهذا الملف.", reply_markup=main_menu_keyboard(uid))

    # إعدادات الحماية
    elif data == "sandbox_settings":
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("حدود الموارد (افتراضي)", callback_data="sandbox_default")],
            [InlineKeyboardButton("حدود مرتفعة", callback_data="sandbox_high")],
            [InlineKeyboardButton("حدود منخفضة", callback_data="sandbox_low")],
            [InlineKeyboardButton("الرجوع", callback_data="back_main")]
        ])
        await safe_edit(q, text="اختر إعدادات الحماية:\n- افتراضي: 60 ثانية، 512 ميجا بايت\n- مرتفع: 300 ثانية، 2048 ميجا بايت\n- منخفض: 30 ثانية، 256 ميجا بايت", reply_markup=keyboard)

    elif data.startswith("sandbox_"):
        if uid != OWNER_ID:
            await q.answer("غير مصرح", show_alert=True)
            return
        settings = data.split("_")[1]
        if settings == "default":
            timeout = 60
            memory = 512
        elif settings == "high":
            timeout = 300
            memory = 2048
        else:  # low
            timeout = 30
            memory = 256
        # حفظ الإعدادات في ملف
        sandbox_config = {"timeout": timeout, "memory": memory}
        with open(os.path.join(BASE_DIR, "sandbox_config.json"), 'w') as f:
            json.dump(sandbox_config, f)
        await safe_edit(q, text=f"تم تعيين إعدادات الحماية:\nالحد الزمني: {timeout} ثانية\nالحد الأقصى للذاكرة: {memory} ميجا بايت", reply_markup=main_menu_keyboard(uid))

    # الرجوع للقائمة الرئيسية
    elif data == "back_main":
        await safe_edit(q, text=CAPTION_TEXT, reply_markup=main_menu_keyboard(uid))

    # تثبيت مكتبة مفقودة
    elif data.startswith("install:"):
        _, lib, folder = data.split(":", 2)
        info = uploaded_files.get(folder) or pending_uploads.get(folder)
        if info and "path" in info:
            path = info["path"]
        else:
            work_dir = os.path.join(BASE_DIR, folder)
            py_files = [f for f in os.listdir(work_dir) if f.endswith('.py')]
            if py_files:
                path = os.path.join(work_dir, py_files[0])
            else:
                path = os.path.join(work_dir, "main.py")
        if not os.path.exists(path):
            await safe_edit(q, text="الملف لم يعد موجوداً.", reply_markup=main_menu_keyboard(uid))
            return
        await safe_edit(q, text=f"جارٍ تثبيت {lib}...")
        p = await asyncio.create_subprocess_exec("python3", "-m", "pip", "install", lib,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, err = await p.communicate()
        if p.returncode != 0:
            await safe_edit(q, text=f"فشل تثبيت {lib}:\n{err.decode()[:1000]}", reply_markup=main_menu_keyboard(uid))
            return
        await safe_edit(q, text=f"تم تثبيت {lib}، جاري إعادة التشغيل...")
        await stop_process_by_user(uid)
        await q.message.delete()
        await handle_uploaded_file(uid, folder, path, context)

    # موافقة المالك
    elif data.startswith("approve_"):
        if uid != OWNER_ID:
            await q.answer("للمالك فقط", show_alert=True)
            return
        _, target_uid, folder = data.split("_", 2)
        target_uid = int(target_uid)
        info = pending_uploads.pop(folder, None)
        if not info:
            await safe_edit(q, text="الطلب انتهت صلاحيته.")
            return
        uploaded_files[folder] = {
            "user_id": target_uid,
            "path": info["path"],
            "filename": info.get("filename", os.path.basename(info["path"])),
            "owner_name": info.get("name", str(target_uid))
        }
        await safe_edit(q, text="تمت الموافقة. جاري تشغيل الملف...")
        await context.bot.send_message(target_uid, "تمت الموافقة على ملفك وجاري تشغيله.")
        await handle_uploaded_file(target_uid, folder, info["path"], context)

    # رفض المالك
    elif data.startswith("reject_"):
        if uid != OWNER_ID:
            await q.answer("للمالك فقط", show_alert=True)
            return
        _, target_uid, folder = data.split("_", 2)
        target_uid = int(target_uid)
        info = pending_uploads.pop(folder, None)
        if info:
            shutil.rmtree(os.path.join(BASE_DIR, folder), ignore_errors=True)
        await safe_edit(q, text="تم رفض الملف.")
        await context.bot.send_message(target_uid, "عذراً، تم رفض ملفك من قبل المالك.")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_authorized(uid) or user_states.get(uid) != "awaiting_file":
        return
    user_states.pop(uid, None)

    # التعامل مع روابط التحميل
    if update.message.text and update.message.text.startswith('http'):
        url = update.message.text.strip()
        await update.message.reply_text(f"جاري تحميل الملف من الرابط...")
        folder = uuid.uuid4().hex[:8]
        work_dir = os.path.join(BASE_DIR, folder)
        os.makedirs(work_dir, exist_ok=True)
        
        # تحميل الملف باستخدام wget أو curl
        proc = await asyncio.create_subprocess_exec(
            "wget", "-O", os.path.join(work_dir, "downloaded_file"), url,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, err = await proc.communicate()
        if proc.returncode != 0:
            await update.message.reply_text(f"فشل تحميل الملف: {err.decode()[:500]}")
            shutil.rmtree(work_dir, ignore_errors=True)
            return
        
        # تحديد نوع الملف
        downloaded_path = os.path.join(work_dir, "downloaded_file")
        if downloaded_path.endswith('.zip') or url.endswith('.zip'):
            # معالجة ZIP
            try:
                extract_zip(downloaded_path, work_dir)
                os.remove(downloaded_path)
                main_path = find_py_file(work_dir)
                if not main_path:
                    await update.message.reply_text("لم أجد أي ملف بايثون في المشروع.")
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return
                await process_uploaded_file(uid, folder, main_path, work_dir, context, update)
            except Exception as e:
                await update.message.reply_text(f"حدث خطأ: {e}")
                shutil.rmtree(work_dir, ignore_errors=True)
        else:
            # ملف بايثون عادي
            main_path = downloaded_path
            await process_uploaded_file(uid, folder, main_path, work_dir, context, update)
        return

    # التعامل مع الملفات المرفوعة عبر التليجرام
    doc = update.message.document
    if not doc.file_name:
        await update.message.reply_text("الملف بدون اسم")
        return

    folder = uuid.uuid4().hex[:8]
    work_dir = os.path.join(BASE_DIR, folder)
    os.makedirs(work_dir, exist_ok=True)

    file_path = os.path.join(work_dir, doc.file_name)
    file = await doc.get_file()
    await file.download_to_drive(file_path)

    if doc.file_name.endswith('.zip'):
        try:
            extract_zip(file_path, work_dir)
            os.remove(file_path)
            main_path = find_py_file(work_dir)
            if not main_path:
                await update.message.reply_text("لم أجد أي ملف بايثون في المشروع.")
                shutil.rmtree(work_dir, ignore_errors=True)
                return
            await process_uploaded_file(uid, folder, main_path, work_dir, context, update)
        except Exception as e:
            await update.message.reply_text(f"حدث خطأ أثناء معالجة الملف المضغوط: {e}")
            shutil.rmtree(work_dir, ignore_errors=True)
    elif doc.file_name.endswith('.py'):
        main_path = file_path
        await process_uploaded_file(uid, folder, main_path, work_dir, context, update)
    else:
        await update.message.reply_text("نوع الملف غير مدعوم. أرسل ملف .py أو .zip أو رابط تحميل.")
        shutil.rmtree(work_dir, ignore_errors=True)

def find_py_file(work_dir):
    """البحث عن أي ملف بايثون في المجلد."""
    for root, dirs, files in os.walk(work_dir):
        for f in files:
            if f.endswith('.py'):
                return os.path.join(root, f)
    return None

def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

async def process_uploaded_file(uid, folder, main_path, work_dir, context, update):
    """معالجة الملف المرفوع وإرساله للمالك للموافقة."""
    # تثبيت المكتبات بشكل ذكي
    req_path = os.path.join(work_dir, "requirements.txt")
    if os.path.exists(req_path) or os.path.exists(os.path.join(work_dir, "Pipfile")) or os.path.exists(os.path.join(work_dir, "pyproject.toml")):
        await update.message.reply_text("جاري تثبيت المكتبات...")
        await smart_install_libraries(work_dir, uid, context)
    
    pending_uploads[folder] = {
        "user_id": uid,
        "path": main_path,
        "name": update.effective_user.first_name or str(uid),
        "filename": os.path.basename(main_path)
    }
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("موافقة", callback_data=f"approve_{uid}_{folder}"),
         InlineKeyboardButton("رفض", callback_data=f"reject_{uid}_{folder}")]
    ])
    await context.bot.send_message(
        OWNER_ID,
        f"طلب رفع ملف جديد من @{update.effective_user.username or uid}\nالملف: {os.path.basename(main_path)}\nالمجلد: {folder}",
        reply_markup=keyboard
    )
    await update.message.reply_text("تم رفع الملف. في انتظار موافقة المالك.")

async def handle_uploaded_file(uid, folder, script_path, context, restart=False):
    """تشغيل الملف بعد الموافقة."""
    await stop_process_by_user(uid)
    
    # تحميل متغيرات البيئة
    envs = load_envs().get(str(uid), {})
    env = os.environ.copy()
    env.update(envs)
    
    # إعدادات الحماية
    sandbox_config = {}
    config_path = os.path.join(BASE_DIR, "sandbox_config.json")
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            sandbox_config = json.load(f)
    timeout = sandbox_config.get("timeout", 60)
    memory = sandbox_config.get("memory", 512)
    
    # تحديث حدود الموارد
    set_resource_limits(timeout_seconds=timeout, memory_mb=memory)
    
    # وضع التصحيح - إضافة متغير البيئة
    if debug_modes.get(uid, False):
        env["PYTHONDEBUG"] = "1"
    
    try:
        proc = await asyncio.create_subprocess_exec(
            "python3", "-u", script_path,  # -u لتعطيل التخزين المؤقت للمخرجات
            cwd=os.path.join(BASE_DIR, folder),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
    except Exception as e:
        await context.bot.send_message(uid, f"فشل التشغيل: {e}")
        return

    start_time = time.time()
    running_processes[uid] = {"proc": proc, "folder": folder, "path": script_path, "start_time": start_time}

    # بدء مراقبة المخرجات بشكل فوري
    output_task = asyncio.create_task(stream_output(proc, uid, context, folder))
    process_streams[uid] = {"task": output_task, "stop": False}

    async def monitor():
        out_lines, err_lines = await output_task
        runtime = time.time() - start_time
        success = proc.returncode == 0
        
        # حفظ المخرجات في ملف
        output_file = os.path.join(OUTPUTS_DIR, f"{folder}_output.txt")
        with open(output_file, 'w') as f:
            f.write("=== STDOUT ===\n")
            f.write("\n".join(out_lines))
            f.write("\n=== STDERR ===\n")
            f.write("\n".join(err_lines))
        
        if success:
            if not restart:
                await context.bot.send_message(uid, f"تم الانتهاء بنجاح. وقت التشغيل: {runtime:.2f} ثانية")
                # إرسال المخرجات كملف إن كانت طويلة
                if len(out_lines) > 50:
                    await context.bot.send_document(uid, open(output_file, 'rb'), filename=f"{folder}_output.txt")
        else:
            error_msg = "\n".join(err_lines) if err_lines else "خطأ غير معروف"
            await context.bot.send_message(uid, f"انتهى التنفيذ بخطأ. وقت التشغيل: {runtime:.2f} ثانية")
            if len(error_msg) < 4000:
                await context.bot.send_message(uid, f"الخطأ:\n{error_msg[:4000]}")
            else:
                await context.bot.send_document(uid, open(output_file, 'rb'), filename=f"{folder}_error.txt")
        
        update_stats(uid, success=success, error_msg=error_msg if not success else None, runtime=runtime)
        running_processes.pop(uid, None)
        process_streams.pop(uid, None)

    asyncio.create_task(monitor())
    if not restart:
        await context.bot.send_message(uid, f"تم تشغيل الملف في الخلفية.\nالمجلد: {folder}")
    else:
        await context.bot.send_message(uid, f"تم إعادة تشغيل الملف.\nالمجلد: {folder}")

def extract_missing_library(err_text):
    m = re.search(r"No module named '([^']+)'", err_text)
    return m.group(1) if m else None

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_authorized(uid):
        return
    
    state = user_states.get(uid)
    
    # تثبيت مكتبة
    if state == "awaiting_library":
        user_states.pop(uid, None)
        lib = update.message.text.strip()
        if not lib:
            await update.message.reply_text("أرسل اسم المكتبة.")
            return
        await update.message.reply_text(f"جارٍ تثبيت {lib}...")
        p = await asyncio.create_subprocess_exec("python3", "-m", "pip", "install", lib,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await p.communicate()
        if p.returncode == 0:
            await update.message.reply_text(f"تم تثبيت {lib} بنجاح.")
            # تحديث الإحصائيات
            uid = str(uid)
            if uid in user_stats:
                user_stats[uid]["libraries"] = list(set(user_stats[uid]["libraries"] + [lib]))
                save_stats()
        else:
            await update.message.reply_text(f"فشل تثبيت {lib}:\n{err.decode()[:1000]}")
        await send_main_menu(update, context)
    
    # تعيين متغيرات البيئة
    elif state == "awaiting_env":
        user_states.pop(uid, None)
        text = update.message.text.strip()
        envs = load_envs()
        uid_str = str(uid)
        if uid_str not in envs:
            envs[uid_str] = {}
        for line in text.split('\n'):
            if '=' in line:
                key, value = line.split('=', 1)
                envs[uid_str][key.strip()] = value.strip()
        save_envs(envs)
        await update.message.reply_text("تم حفظ متغيرات البيئة بنجاح.")
        await send_main_menu(update, context)
    
    # روابط التحميل
    elif state == "awaiting_file" and update.message.text and update.message.text.startswith('http'):
        await handle_document(update, context)

# ==================== تشغيل البوت ====================
if __name__ == "__main__":
    load_stats()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    print("البوت يعمل مع جميع الميزات المتقدمة...")
    app.run_polling(drop_pending_updates=True)
