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

/** 从常见头部尽量还原 To；没就用传入的 fallback（信封收件人） */
function resolveRecipientFromHeaders(headers, fallbackTo) {
  const keys = [
    'x-original-to','original-recipient','delivered-to',
    'envelope-to','x-receiver','x-forwarded-to'
  ];
  for (const k of keys) {
    const v = headers.get(k);
    if (v) {
      const em = extractFirstEmail(v);
      if (em) return normalizeEmail(em);
    }
  }
  return normalizeEmail(fallbackTo);
}

/** 接收域 -> 展示域（cPanel 域） */
function mapToDisplayDomain(envelopeDomain, env) {
  const map = safeParseJSON(env.DISPLAY_DOMAIN_MAP, {});
  const d = (envelopeDomain || '').toLowerCase();
  const v = map[d];
  if (!v) return d;
  if (typeof v === 'string') return v.toLowerCase();
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
      ruleEmail,
      ruleType,
      r2Domain,
      noRecipient
    } = await settingService.query({ env });

    if (receive === settingConst.receive.CLOSE) {
      console.log('[MAIL] receive=CLOSE -> drop');
      return;
    }

    const headers = message.headers;
    const envelopeTo = normalizeEmail(message.to); // Cloudflare Runtime envelope rcptTo
    const { local: envLocal, domain: envDomain } = parseAddr(envelopeTo);

    // 允许的接收域白名单
    const allow = safeParseJSON(env.ALLOWED_ENVELOPE_DOMAINS, []);
    if (Array.isArray(allow) && allow.length > 0 && !allow.map(s => s.toLowerCase()).includes(envDomain)) {
      console.log('[MAIL] envelope domain not allowed:', envDomain);
      return;
    }

    // 解析 To（若上游保留了原 To）
    const headerTo = headers.get('to');
    const resolvedTo = resolveRecipientFromHeaders(headers, envelopeTo);
    const { local: hdrLocal } = parseAddr(resolvedTo);
    const localPart = hdrLocal || envLocal;

    // 接收域 -> 展示域
    const displayDomain = mapToDisplayDomain(envDomain, env);
    let finalTo = normalizeEmail(`${localPart}@${displayDomain}`);

    console.log('[MAIL] envelope.to =', envelopeTo);
    console.log('[MAIL] header.to   =', headerTo);
    console.log('[MAIL] localPart    =', localPart);
    console.log('[MAIL] displayDomain=', displayDomain);
    console.log('[MAIL] finalTo      =', finalTo);

    // 读取原始邮件并解析
    const reader = message.raw.getReader();
    let content = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      content += new TextDecoder().decode(value);
    }
    const parsed = await PostalMime.parse(content);

    // 账号匹配（以展示域地址为准）
    let account = await accountService.selectByEmailIncludeDel({ env }, finalTo);
    console.log('[MAIL] account hit? ', !!account, account?.email);

    // 如果没有账号，是否强制入库？
    const acceptUnknown = String(env.ACCEPT_UNKNOWN_RECIPIENTS || 'true').toLowerCase() === 'true';
    if (!account && !acceptUnknown && noRecipient === settingConst.noRecipient.CLOSE) {
      console.log('[MAIL] no account & noRecipient=CLOSE -> drop');
      return;
    }

    // 权限 / 黑名单
    if (account && account.email !== env.admin) {
      let { banEmail, banEmailType, availDomain } =
        await roleService.selectByUserId({ env }, account.userId);

      if (!roleService.hasAvailDomainPerm(availDomain, finalTo)) {
        console.log('[MAIL] hasAvailDomainPerm=false -> drop');
        return;
      }

      banEmail = (banEmail || '').split(',').filter(Boolean);
      for (const item of banEmail) {
        if (verifyUtils.isDomain(item)) {
          const banDomain = item.toLowerCase();
          const fromDomain = emailUtils.getDomain((parsed.from?.address || '').toLowerCase());
          if (banDomain === fromDomain) {
            if (banEmailType === roleConst.banEmailType.ALL) return;
            if (banEmailType === roleConst.banEmailType.CONTENT) {
              parsed.html = 'The content has been deleted';
              parsed.text = 'The content has been deleted';
              parsed.attachments = [];
            }
          }
        } else {
          if (item.toLowerCase() === (parsed.from?.address || '').toLowerCase()) {
            if (banEmailType === roleConst.banEmailType.ALL) return;
            if (banEmailType === roleConst.banEmailType.CONTENT) {
              parsed.html = 'The content has been deleted';
              parsed.text = 'The content has been deleted';
              parsed.attachments = [];
            }
          }
        }
      }
    }

    // 取显示名
    const toName =
      (parsed.to?.find?.(i => {
        const a = (i.address || '').toLowerCase();
        return a === finalTo || a === envelopeTo;
      })?.name) || '';

    // 入库参数（只“收”，不“转”）
    const params = {
      toEmail: finalTo,
      toName: toName,
      sendEmail: parsed.from?.address || '',
      name: parsed.from?.name || emailUtils.getName(parsed.from?.address || ''),
      subject: parsed.subject,
      content: parsed.html,
      text: parsed.text,
      cc: parsed.cc ? JSON.stringify(parsed.cc) : '[]',
      bcc: parsed.bcc ? JSON.stringify(parsed.bcc) : '[]',
      recipient: JSON.stringify(parsed.to || []),
      inReplyTo: parsed.inReplyTo,
      relation: parsed.references,
      messageId: parsed.messageId,
      userId: account ? account.userId : 0,
      accountId: account ? account.accountId : 0,
      isDel: isDel.DELETE,
      status: emailConst.status.SAVING
    };

    // 附件处理
    const attachments = [];
    const cidAttachments = [];
    for (let item of (parsed.attachments || [])) {
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

    attachments.forEach(a => {
      a.emailId = emailRow.emailId;
      a.userId = emailRow.userId;
      a.accountId = emailRow.accountId;
    });
    if (attachments.length > 0 && env.r2) {
      await attService.addAtt({ env }, attachments);
    }

    emailRow = await emailService.completeReceive(
      { env },
      account ? emailConst.status.RECEIVE : emailConst.status.NOONE,
      emailRow.emailId
    );

    // 规则过滤（如开启）
    if (ruleType === settingConst.ruleType.RULE) {
      const emails = (ruleEmail || '').split(',').map(e => (e || '').trim().toLowerCase());
      if (!emails.includes(finalTo) && !emails.includes(envelopeTo)) {
        console.log('[MAIL] ruleType=RULE but not listed -> drop');
        return;
      }
    }

    // 不做任何 forward，避免回环
    console.log('[MAIL] saved OK -> emailId', emailRow.emailId);

    // Telegram（如开启）
    if (tgBotStatus === settingConst.tgBotStatus.OPEN && tgChatId) {
      const msg = `<b>${params.subject || ''}</b>

<b>发件人：</b>${params.name}\t&lt;${params.sendEmail}&gt;
<b>收件人：\u200B</b>${finalTo}
<b>时间：</b>${dayjs.utc(emailRow.createTime).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm')}

${params.text || emailUtils.htmlToText(params.content) || ''}`;
      try {
        await Promise.all(
          tgChatId.split(',').map(async id => {
            const res = await fetch(`https://api.telegram.org/bot${tgBotToken}/sendMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: id, parse_mode: 'HTML', text: msg })
            });
            if (!res.ok) console.error('[MAIL] telegram fail', id, res.status);
          })
        );
      } catch (e) {
        console.error('[MAIL] telegram exception', e);
      }
    }
  } catch (e) {
    console.error('邮件接收异常: ', e);
  }
}
