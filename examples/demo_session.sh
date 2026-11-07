#!/usr/bin/env bash
# Demo: builds examples/demo.jsonl via the CLI, exercising every record type.
# Usage: bash examples/demo_session.sh && python timeline.py -f examples/demo.jsonl render html
set -euo pipefail
cd "$(dirname "$0")/.."
export TIMELINE_FILE=examples/demo.jsonl
tl() { python timeline.py "$@" >/dev/null; }

tl init "Demo / pwn / heap-notes" --task heap-notes --goal "Получить флаг ctfcup{...} с удалённого сервиса heap-notes" \
   --meta model=zai/glm-5.3 --meta harness=inslopnia --meta config=P+E+R --t 2026-11-06T10:00:00 --force

tl agent P --role P --model zai/glm-5.3 --label "планировщик" --start --t +0s
tl phase begin recon --t +0s
tl event P "Прочитал условие и бинарь, checksec: NX, PIE, Full RELRO" -k observation --t +40s --tokens 18000
tl hyp add "UAF в delete_note: указатель не обнуляется" --hid H1 -a P --t +1m30s
tl hyp add "format string в edit_title" --hid H2 -a P --t +1m40s
tl dag add recon -o "Разведка: собрать бинарь, libc, прогнать базовые сценарии" --assignee E:recon --t +2m
tl dag add research -o "Найти технику tcache poisoning для glibc 2.39 (safe-linking)" --assignee R:research --t +2m
tl dag add exploit -o "Эксплойт UAF → tcache poisoning → __free_hook/ROP" --deps recon,research --assignee E:exploit --t +2m
tl agent E:recon --role E --model zai/glm-5.3 --parent P --label "разведка" --t +2m
tl agent R:research --role R --model zai/glm-5.3 --parent P --label "исследователь" --t +2m
tl dag status recon running --t +2m20s
tl dag status research running --t +2m25s
tl event E:recon "Запустил бинарь локально в docker с той же libc" -s recon --t +4m --tokens 32000
tl event E:recon "delete_note не зануляет указатель — UAF подтверждён" -k milestone -s recon --to P --t +9m --tokens 41000 \
   -d "free(notes[i]) без notes[i]=NULL; повторный show печатает fd-указатель tcache"
tl hyp set H1 confirmed -e "show() после delete печатает tcache fd" -a E:recon --t +9m10s
tl event E:recon "edit_title использует snprintf с фиксированным форматом" -k observation -s recon --t +10m --tokens 12000
tl hyp set H2 refuted -e "формат строки фиксированный" -a E:recon --t +10m30s
tl dag status recon done --by runtime --t +11m
tl tokens E:recon 140000 --cumulative --in 120000 --out 20000 --cost 0.21 --t +11m
tl incident add "E:recon: pi потерял stdout финального события, шаг почти помечен failed" --sev high -c harness -a E:recon -s recon \
   -d "run_streaming на Windows дропнул последнее событие; спас TASK_COMPLETE из pi-sessions" --impact "ложный провал шага" \
   --workaround "читать pi-sessions вместо stdout" --t +11m05s
tl phase end recon --t +11m30s
tl phase begin exploit --t +11m30s

tl event R:research "Нашёл writeup: safe-linking обходится утечкой heap base через UAF" -k result -s research --to P --t +13m --tokens 54000
tl dag status research done --by runtime --t +13m30s
tl tokens R:research 90000 --cumulative --cost 0.12 --t +13m30s
tl event P "Решение: вместо __free_hook (удалён в 2.34+) — FSOP через _IO_list_all" -k decision --t +14m --tokens 9000
tl dag update exploit -o "Эксплойт UAF → leak heap/libc → tcache poisoning → FSOP" --note "цель поменялась: нет __free_hook" --t +14m10s
tl dag add leak -o "Стабильная утечка heap base и libc" --assignee E:leak --t +14m20s
tl dag dep-add exploit leak --t +14m25s
tl agent E:leak --role E --model zai/glm-5.3 --parent P --t +14m20s
tl dag status leak running --t +14m40s
tl event E:leak "heap base утёк через show() освобождённого чанка" -k milestone -s leak --to P --t +19m --tokens 60000
tl event E:leak "libc leak через unsorted bin" -k milestone -s leak --t +22m --tokens 45000
tl dag status leak done --by runtime --t +23m
tl tokens E:leak 160000 --cumulative --cost 0.24 --t +23m
tl dag status exploit running --t +23m20s
tl agent E:exploit --role E --model zai/glm-5.3 --parent P --t +23m20s
tl event E:exploit "Пишу FSOP-цепочку (house of apple 2)" -s exploit --t +27m --tokens 70000
tl event E:exploit "Локально получил шелл" -k milestone -s exploit --to P --t +33m --tokens 50000
tl event E:exploit "Удалённо: EOF после 3-го запроса — таймаут сервиса" -k error -s exploit --t +36m --tokens 20000
tl incident add "Удалённый сервис рвёт соединение через 5 c" --sev medium -c env -a E:exploit -s exploit --t +36m10s
tl dag status exploit failed --by runtime -r "timeout на удалённом сервисе" --t +37m
tl event P "Steer: ускорить эксплойт, отправлять всё одним sendline-пакетом" -k message --to E:exploit -s exploit --t +37m30s --tokens 6000
tl incident update I2 --note "P рекомендовал батчинг запросов" --status investigating --t +37m40s
tl dag status exploit running --reason "attempt 2" --t +38m
tl event E:exploit "Батчинг сработал, шелл на удалённом" -k milestone -s exploit --to P --t +43m --tokens 40000
tl incident resolve I2 --note "батчинг payload'ов уложился в 5 c" --t +43m10s
tl flag "ctfcup{fake_test_flag}" --status rejected -a E:exploit --note "прочитал /flag.bak — приманка" --t +44m
tl flag "ctfcup{h0us3_0f_4ppl3_2_w1th_s4f3_l1nk1ng}" --status accepted -a E:exploit --t +45m
tl dag status exploit done --by runtime --t +45m30s
tl tokens E:exploit 230000 --cumulative --cost 0.36 --t +45m30s
tl dag add cleanup -o "Удалить временные контейнеры" --t +46m
tl dag remove cleanup -r "не нужно: прогон завершается" --t +46m30s
tl phase end exploit --t +47m
tl tokens P 95000 --in 80000 --out 15000 --cost 0.14 --t +47m
tl outcome solved --flag "ctfcup{h0us3_0f_4ppl3_2_w1th_s4f3_l1nk1ng}" --summary "UAF → leak → tcache poisoning → FSOP; удалённо — батчинг из-за таймаута" --by P --t +47m10s
tl agent P --stop --status done --t +47m20s
echo "ok: $TIMELINE_FILE"
