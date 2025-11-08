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

/* ------------------------- helpers ------------------------- */

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
function safeJSON(s, def) {
  try { return s ? JSON.parse(s) : def; } catch { return def; }
}
/** 还原 To：尽量用保留头，失败就用 fallback（envelope rcptTo） */
function resolveRecipientFromHeaders(headers, fallbackTo) {
  const keys = ['x-original-to','original-recipient','delivered-to','envelope-to','x-receiver','x-forwarded-to'];
  for (const k of keys) {
    const v = headers.get(k);
    if (v) {
      const em = extractFirstEmail(v);
      if (em) return normalizeEmail(em);
    }
  }
  return normalizeEmail(fallbackTo);
}
/** 接收域 -> 展示域 */
function mapToDisplayDomain(envelopeDomain, env) {
  const map = safeJSON(env.DISPLAY_DOMAIN_MAP, {});
  const d = (envelopeDomain || '').toLowerCase();
  const v = map[d];
  if (!v) return d;
  if (typeof v === 'string') return v.toLowerCase();
  if (Array.isArray(v) && v.length > 0) return String(v[0]).toLowerCase();
  return d;
}

/* ----------------------------- main ----------------------------- */

export async function email(message, env, ctx) {
  try {
    const settings = await settingService.query({ env });
    const {
      receive,
      tgBotToken, tgChatId, tgBotStatus,
      ruleEmail, ruleType, r2Domain
    } = settings;

    if (receive === settingConst.receive.CLOSE) {
      console.log('[MAIL] receive=CLOSE -> drop');
      return;
    }

    // 0) envelope & headers（Email Workers Runtime 提供）
    const envelopeTo = normalizeEmail(message.to); // 信封收件人（用于域映射）
    const headers = message.headers;
    const { local: envLocal, domain: envDomain } = parseAddr(envelopeTo);

    // 1) 仅处理允许的接收域（可选）
    const allow = safeJSON(env.ALLOWED_ENVELOPE_DOMAINS, []);
    if (Array.isArray(allow) && allow.length > 0 && !allow.map(s => s.toLowerCase()).includes(envDomain)) {
      console.log('[MAIL] not allowed envelope domain:', envDomain);
      return;
    }

    // 2) 解析 To 的本地部分（若上游保留原 To 则优先）
    const headerTo = headers.get('to');
    const resolvedTo = resolveRecipientFromHeaders(headers, envelopeTo);
    const { local: hdrLocal } = parseAddr(resolvedTo);
    const localPart = hdrLocal || envLocal;

    // 3) 接收域 -> 展示域（cPanel 域） & 目标展示地址
    const displayDomain = mapToDisplayDomain(envDomain, env);
    let targetDisplayAddr = normalizeEmail(`${localPart}@${displayDomain}`);

    // 4) 读取原始报文并解析
    const reader = message.raw.getReader();
    let raw = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      raw += new TextDecoder().decode(value);
    }
    const parsed = await PostalMime.parse(raw);

    console.log('[MAIL] from(Envelope)=', message.from);
    console.log('[MAIL] envelope.to   =', envelopeTo);
    console.log('[MAIL] header.to     =', headerTo);
    console.log('[MAIL] localPart     =', localPart);
    console.log('[MAIL] displayDomain =', displayDomain);
    console.log('[MAIL] targetDisplay =', targetDisplayAddr);

    // 5) 账号匹配（先找 exact 展示地址）
    let account = await accountService.selectByEmailIncludeDel({ env }, targetDisplayAddr);
    let finalTo = targetDisplayAddr;

    // 5.1 若找不到，使用“每个展示域的汇聚账号（sink）”兜底
    if (!account) {
      const sinkMap = safeJSON(env.DISPLAY_SINK_ACCOUNT_MAP, {}); // { "ccc.sandbox.lib.uci.edu": "root@ccc.sandbox.lib.uci.edu", ... }
      const sinkEmail = normalizeEmail(sinkMap[displayDomain] || env.admin || '');
      if (sinkEmail) {
        const maybe = await accountService.selectByEmailIncludeDel({ env }, sinkEmail);
        if (maybe) {
          account = maybe;
          finalTo = sinkEmail; // 入库归档到汇聚账号，保证 UI 可见
          console.log('[MAIL] sink fallback ->', sinkEmail);
        } else {
          console.log('[MAIL] sink email not found in accounts:', sinkEmail);
        }
      } else {
        console.log('[MAIL] no sink configured; keep targetDisplay even without account');
      }
    }

    // 6) 是否强制可见
    const FORCE_SAVE = String(env.FORCE_SAVE || 'true').toLowerCase() === 'true';
    const acceptUnknown = String(env.ACCEPT_UNKNOWN_RECIPIENTS || 'true').toLowerCase() === 'true';

    if (!account && !acceptUnknown && settings.noRecipient === settingConst.noRecipient.CLOSE) {
      console.log('[MAIL] no account & noRecipient=CLOSE & acceptUnknown=false -> drop');
      return;
    }

    // 7) 权限/黑名单（仅在命中账号时检查）
    if (account && account.email !== env.admin) {
      let { banEmail, banEmailType, availDomain } =
        await roleService.selectByUserId({ env }, account.userId);

      const hasPerm = roleService.hasAvailDomainPerm(availDomain, finalTo);
      if (!hasPerm) {
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

    // 8) 规则模式：FORCE_SAVE 时跳过规则拦截（保证可见）
    if (!FORCE_SAVE && ruleType === settingConst.ruleType.RULE) {
      const emails = (ruleEmail || '').split(',').map(e => (e || '').trim().toLowerCase());
      if (!emails.includes(finalTo) && !emails.includes(envelopeTo)) {
        console.log('[MAIL] ruleType=RULE but not listed -> drop');
        return;
      }
    }

    // 9) 入库（只“收”，不再外转）
    const toName =
      (parsed.to?.find?.(i => {
        const a = (i.address || '').toLowerCase();
        return a === finalTo || a === envelopeTo;
      })?.name) || '';

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

    // 附件
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
    if (attachments.length > 0 && env.r2) await attService.addAtt({ env }, attachments);

    // 设为 RECEIVE 以确保前端可见（FORCE_SAVE 时即便无账号也标记为 RECEIVE）
    const finalStatus =
      FORCE_SAVE ? emailConst.status.RECEIVE :
      (account ? emailConst.status.RECEIVE : emailConst.status.NOONE);

    emailRow = await emailService.completeReceive(
      { env },
      finalStatus,
      emailRow.emailId
    );

    console.log('[MAIL] saved OK -> emailId', emailRow.emailId, 'status=', finalStatus, 'finalTo=', finalTo);

    // 不做 forward()，避免回环
    // Telegram 推送如需保留，可在 settings.tgBotStatus=OPEN 时继续发送
    if (settings.tgBotStatus === settingConst.tgBotStatus.OPEN && settings.tgChatId) {
      const msg = `<b>${params.subject || ''}</b>

<b>发件人：</b>${params.name}\t&lt;${params.sendEmail}&gt;
<b>收件人：\u200B</b>${finalTo}
<b>时间：</b>${dayjs.utc(emailRow.createTime).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm')}

${params.text || emailUtils.htmlToText(params.content) || ''}`;
      try {
        await Promise.all(
          settings.tgChatId.split(',').map(async id => {
            const res = await fetch(`https://api.telegram.org/bot${settings.tgBotToken}/sendMessage`, {
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
    console.error('邮件接收异常:', e);
  }
}
