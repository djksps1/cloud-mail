import PostalMime from 'postal-mime';
import emailService from '../service/email-service';
import accountService from '../service/account-service';
import settingService from '../service/setting-service';
import attService from '../service/att-service';
import constant from '../const/constant';
import fileUtils from '../utils/file-utils';
import { emailConst, isDel, roleConst, settingConst } from '../const/entity-const';
import emailUtils from '../utils/email-utils';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';
import roleService from '../service/role-service';
import verifyUtils from '../utils/verify-utils';

dayjs.extend(utc);
dayjs.extend(timezone);

/* ------------------------- 工具函数 ------------------------- */

function extractFirstEmail(s) {
  if (!s) return null;
  const angle = s.match(/<\s*([^>]+)\s*>/);
  if (angle && angle[1]) return angle[1].trim();
  const m = s.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}/);
  return m ? m[0] : null;
}

function normalizeEmail(addr, dropPlus = true) {
  if (!addr) return null;
  let [local, domain] = addr.trim().toLowerCase().split('@');
  if (!local || !domain) return null;
  if (dropPlus && local.includes('+')) local = local.split('+')[0];
  return `${local}@${domain}`;
}

function parseAddr(addr) {
  const n = normalizeEmail(addr || '');
  if (!n) return { local: '', domain: '' };
  const i = n.lastIndexOf('@');
  return { local: n.slice(0, i), domain: n.slice(i + 1) };
}

function safeParseJSON(s, def) {
  try { return s ? JSON.parse(s) : def; } catch { return def; }
}

function uniq(arr) {
  const set = new Set();
  const out = [];
  for (const x of arr) {
    const k = (x || '').toLowerCase();
    if (k && !set.has(k)) { set.add(k); out.push(k); }
  }
  return out;
}

/** 有些转发链会保留原收件人到这些头；用于提取本地部分 */
function resolveRecipientFromHeaders(headers, fallbackTo) {
  const keys = [
    'x-original-to', 'original-recipient', 'delivered-to',
    'envelope-to', 'x-receiver', 'x-forwarded-to'
  ];
  for (const k of keys) {
    const v = headers.get(k);
    if (v) {
      const em = extractFirstEmail(v);
      if (em) return normalizeEmail(em);
    }
  }
  if (fallbackTo) {
    const em = extractFirstEmail(fallbackTo);
    if (em) return normalizeEmail(em);
  }
  return null;
}

/** 根据接收域（envelope domain）映射到“展示域”（cPanel 域） */
function mapToDisplayDomain(envelopeDomain, env) {
  const map = safeParseJSON(env.DISPLAY_DOMAIN_MAP, {}); // { "recv.example": "cpanel.example" }
  const d = (envelopeDomain || '').toLowerCase();
  const v = map[d];
  if (!v) return d;                        // 无映射则用原域
  if (typeof v === 'string') return v.toLowerCase();
  // 容错：如果有人误填成数组，就取第一个
  if (Array.isArray(v) && v.length > 0) return String(v[0]).toLowerCase();
  return d;
}

/* ----------------------------- 主处理逻辑 ----------------------------- */

export async function email(message, env, ctx) {
  try {
    const {
      receive,
      tgBotToken,
      tgChatId,
      tgBotStatus,
      forwardStatus,
      forwardEmail,
      ruleEmail,
      ruleType,
      r2Domain,
      noRecipient
    } = await settingService.query({ env });

    // 关闭接收则直接丢弃
    if (receive === settingConst.receive.CLOSE) return;

    // 1) 解析 envelope 收件人与头部（Email Workers 提供的 email 事件）
    //    message.to 为“信封收件人”，官方 Runtime API 已说明可在 email 事件中获取。 
    //    我们优先用其本地部分，再按映射改域展示。:contentReference[oaicite:3]{index=3}
    const headers = message.headers;
    const envelopeTo = normalizeEmail(message.to);
    const toHeader = headers.get('to');
    // 解析本地部分：先尝试头部（若保留了原 To），否则用 envelope
    const fromHeaders = resolveRecipientFromHeaders(headers, toHeader) || envelopeTo;
    const { local: localFromHdr } = parseAddr(fromHeaders);
    const { local: localFromEnv, domain: envelopeDomain } = parseAddr(envelopeTo);
    const localPart = localFromHdr || localFromEnv;

    // 2) 白名单过滤（可选）
    const allowDomains = safeParseJSON(env.ALLOWED_ENVELOPE_DOMAINS, []);
    if (Array.isArray(allowDomains) && allowDomains.length > 0) {
      const allow = allowDomains.map(d => String(d).toLowerCase());
      if (!allow.includes(envelopeDomain)) {
        // 不在接收白名单，直接丢弃（也可以改成 message.setReject('not allowed')）
        return;
      }
    }

    // 3) “接收域 → 展示域”映射：local@接收域 => local@展示域（只为入库显示，不做实际投递）
    const displayDomain = mapToDisplayDomain(envelopeDomain, env);
    const finalTo = normalizeEmail(`${localPart}@${displayDomain}`);

    // 4) 读取原始邮件并解析
    const reader = message.raw.getReader();
    let content = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      content += new TextDecoder().decode(value);
    }
    const email = await PostalMime.parse(content); // 官方库支持在 Email Workers 解析 RFC822 原文。:contentReference[oaicite:4]{index=4}

    // 5) 找账户：按“展示域地址”查
    let account = await accountService.selectByEmailIncludeDel({ env }, finalTo);

    // 若没账户且系统设置为不接收“未知收件人”，则丢弃
    if (!account && noRecipient === settingConst.noRecipient.CLOSE) return;

    // 6) 账户权限与黑名单（保持你的原逻辑）
    if (account && account.email !== env.admin) {
      let { banEmail, banEmailType, availDomain } =
        await roleService.selectByUserId({ env }, account.userId);

      if (!roleService.hasAvailDomainPerm(availDomain, finalTo)) return;

      banEmail = (banEmail || '').split(',').filter(Boolean);
      for (const item of banEmail) {
        if (verifyUtils.isDomain(item)) {
          const banDomain = item.toLowerCase();
          const receiveDomain = emailUtils.getDomain(email.from.address.toLowerCase());
          if (banDomain === receiveDomain) {
            if (banEmailType === roleConst.banEmailType.ALL) return;
            if (banEmailType === roleConst.banEmailType.CONTENT) {
              email.html = 'The content has been deleted';
              email.text = 'The content has been deleted';
              email.attachments = [];
            }
          }
        } else {
          if (item.toLowerCase() === email.from.address.toLowerCase()) {
            if (banEmailType === roleConst.banEmailType.ALL) return;
            if (banEmailType === roleConst.banEmailType.CONTENT) {
              email.html = 'The content has been deleted';
              email.text = 'The content has been deleted';
              email.attachments = [];
            }
          }
        }
      }
    }

    // 7) 取显示名：优先匹配 finalTo 或原始 to
    const toName =
      (email.to?.find?.(i => {
        const a = (i.address || '').toLowerCase();
        return a === finalTo || a === envelopeTo;
      })?.name) || '';

    // 8) 组装入库参数（**关键**：toEmail 用 finalTo，即 cPanel 展示域）
    const params = {
      toEmail: finalTo,
      toName: toName,
      sendEmail: email.from.address,
      name: email.from.name || emailUtils.getName(email.from.address),
      subject: email.subject,
      content: email.html,
      text: email.text,
      cc: email.cc ? JSON.stringify(email.cc) : '[]',
      bcc: email.bcc ? JSON.stringify(email.bcc) : '[]',
      recipient: JSON.stringify(email.to),
      inReplyTo: email.inReplyTo,
      relation: email.references,
      messageId: email.messageId,
      userId: account ? account.userId : 0,
      accountId: account ? account.accountId : 0,
      isDel: isDel.DELETE,
      status: emailConst.status.SAVING
    };

    // 9) 附件处理（保持）
    const attachments = [];
    const cidAttachments = [];
    for (let item of email.attachments) {
      let attachment = { ...item };
      attachment.key =
        constant.ATTACHMENT_PREFIX +
        (await fileUtils.getBuffHash(attachment.content)) +
        fileUtils.getExtFileName(item.filename);
      attachment.size = item.content.length ?? item.content.byteLength;
      attachments.push(attachment);
      if (attachment.contentId) cidAttachments.push(attachment);
    }

    let emailRow = await emailService.receive({ env }, params, cidAttachments, r2Domain);

    attachments.forEach(attachment => {
      attachment.emailId = emailRow.emailId;
      attachment.userId = emailRow.userId;
      attachment.accountId = emailRow.accountId;
    });
    if (attachments.length > 0 && env.r2) await attService.addAtt({ env }, attachments);

    emailRow = await emailService.completeReceive(
      { env },
      account ? emailConst.status.RECEIVE : emailConst.status.NOONE,
      emailRow.emailId
    );

    // 10) 规则过滤（保持）
    if (ruleType === settingConst.ruleType.RULE) {
      const emails = (ruleEmail || '').split(',').map(e => (e || '').trim().toLowerCase());
      if (!emails.includes(finalTo) && !emails.includes(envelopeTo)) return;
    }

    // 11) Telegram 推送（保持）
    if (tgBotStatus === settingConst.tgBotStatus.OPEN && tgChatId) {
      const tgMessage = `<b>${params.subject}</b>

<b>发件人：</b>${params.name}\t&lt;${params.sendEmail}&gt;
<b>收件人：\u200B</b>${finalTo}
<b>时间：</b>${dayjs.utc(emailRow.createTime).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm')}

${params.text || emailUtils.htmlToText(params.content) || ''}
`;
      const ids = tgChatId.split(',');
      await Promise.all(ids.map(async id => {
        try {
          const res = await fetch(`https://api.telegram.org/bot${tgBotToken}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: id, parse_mode: 'HTML', text: tgMessage })
          });
          if (!res.ok) console.error(`Telegram 失败: chatId=${id}, code=${res.status}`);
        } catch (e) { console.error(`Telegram 失败: chatId=${id}`, e); }
      }));
    }

    // 12) 彻底避免回环：不再外转（forwardStatus=OPEN 时也建议忽略）
    // 如果你一定要保留开关，可把下面 return 留着；否则强制不转发：
    if (forwardStatus === settingConst.forwardStatus.OPEN && forwardEmail) {
      // 注：为满足“只在 cloud-mail 显示”的目标，建议将 forwardStatus 置为 CLOSE
      return;
    }

  } catch (e) {
    console.error('邮件接收异常: ', e);
  }
}
