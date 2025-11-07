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

function uniq(arr) {
  const seen = new Set();
  const out = [];
  for (const x of arr) {
    const k = (x || '').toLowerCase();
    if (k && !seen.has(k)) {
      seen.add(k);
      out.push(k);
    }
  }
  return out;
}

function safeParseJSON(s, def) {
  try {
    return s ? JSON.parse(s) : def;
  } catch (e) {
    return def;
  }
}

/** 尽量从转发保留头还原真实收件人，否则退回信封收件人 */
function resolveRecipientFromHeaders(headers, fallbackTo) {
  const keys = [
    'x-original-to',
    'original-recipient',
    'delivered-to',
    'envelope-to',
    'x-receiver',
    'x-forwarded-to'
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

/** 读取发件人绑定（一个 Gmail 中转地址 → 一个或多个目标 cPanel 域） */
function getSenderBinding(env, senderAddr) {
  const bindings = safeParseJSON(env.SENDER_BINDINGS, {}); // 可能是 { "a@gmail.com": "cpanel1.com", "b@gmail.com": { targetDomains:[...], allowedTo:[...], strict:true } }
  // 既尝试完整地址，也尝试去掉 plus 的基地址
  const full = normalizeEmail(senderAddr, false);
  const base = normalizeEmail(senderAddr, true);
  let b = bindings[full] || bindings[base];
  if (!b) return null;

  let targetDomains = [];
  let allowedTo = [];
  let strict = false;

  if (typeof b === 'string') {
    targetDomains = [b.toLowerCase()];
  } else if (typeof b === 'object' && b) {
    if (Array.isArray(b.targetDomains)) targetDomains = b.targetDomains.map(d => String(d).toLowerCase());
    if (typeof b.targetDomain === 'string') targetDomains.push(b.targetDomain.toLowerCase());
    if (Array.isArray(b.allowedTo)) allowedTo = b.allowedTo.map(d => String(d).toLowerCase());
    if (typeof b.strict === 'boolean') strict = b.strict;
  }

  return { targetDomains: uniq(targetDomains), allowedTo: uniq(allowedTo), strict };
}

/**
 * 构造候选地址列表：
 * - senderPreferredDomains：来自“发件人绑定”的优先域（最高优先级）
 * - 之后按收件域映射、全局兜底域、原样域拼接
 */
function buildCandidateAddresses(resolvedTo, env, senderPreferredDomains = []) {
  const { local, domain } = parseAddr(resolvedTo);
  const aliasMap = safeParseJSON(env.RECIPIENT_DOMAIN_MAP, {}); // {alias:string|array}
  const canonList = safeParseJSON(env.CANONICAL_DOMAINS, []);   // ["d1","d2",...]
  const primary = (env.PRIMARY_DOMAIN || '').toLowerCase();

  const candidates = [];

  // 0) 发件人绑定域（最高优先）
  for (const d of senderPreferredDomains || []) {
    candidates.push(`${local}@${d}`);
  }

  // 1) 收件域映射：alias -> 一个或多个真正 cPanel 域
  let mapped = aliasMap[domain];
  if (mapped) {
    if (!Array.isArray(mapped)) mapped = [mapped];
    for (const d of mapped) {
      candidates.push(`${local}@${String(d).toLowerCase()}`);
    }
  }

  // 2) 全局兜底：所有 cPanel 主域集合
  for (const d of canonList || []) {
    candidates.push(`${local}@${String(d).toLowerCase()}`);
  }

  // 3) 最终兜底：PRIMARY_DOMAIN
  if (primary) candidates.push(`${local}@${primary}`);

  // 4) 原样地址（如果你也在 cloud‑mail 建了聚合域账号可以命中）
  candidates.push(`${local}@${domain}`);

  return uniq(candidates);
}

/** 在候选地址中尝试命中 cloud‑mail 账号；命中则返回 {account, finalTo} */
async function resolveAccountAcrossCandidates(env, candidates) {
  for (const addr of candidates) {
    const acc = await accountService.selectByEmailIncludeDel({ env }, addr);
    if (acc) return { account: acc, finalTo: addr };
  }
  return { account: null, finalTo: candidates[0] || null };
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

    if (receive === settingConst.receive.CLOSE) {
      return;
    }

    // 解析“真实收件人”：优先从保留头，还原原始收件人，否则用 envelope-to（message.to）
    const headers = message.headers;
    const toHeader = headers.get('to');
    const resolvedTo =
      resolveRecipientFromHeaders(headers, toHeader) || normalizeEmail(message.to);

    // 当前信封收件人域，用于 optional 严格校验
    const { domain: envelopeToDomain } = parseAddr(message.to);

    /* ---------- 读取原始邮件并解析 ---------- */
    const reader = message.raw.getReader();
    let content = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      content += new TextDecoder().decode(value);
    }
    const email = await PostalMime.parse(content);

    /* ---------- 发件人绑定（一个 Gmail 中转地址 → 一个 cPanel 域） ---------- */
    const senderBinding = getSenderBinding(env, email.from?.address || '');
    if (senderBinding) {
      // 严格模式：若限制了 allowedTo，则 envelope-to 域必须匹配，否则丢弃
      if (senderBinding.strict && senderBinding.allowedTo.length > 0) {
        if (!senderBinding.allowedTo.includes(envelopeToDomain)) {
          // 这里可以改成 message.setReject('reason')，当前按静默丢弃处理
          return;
        }
      }
    }

    /* ---------- 依据“发件人绑定 + 收件域映射”构造候选地址并匹配账号 ---------- */
    const candidates = buildCandidateAddresses(
      resolvedTo,
      env,
      senderBinding ? senderBinding.targetDomains : []
    );

    let { account, finalTo } = await resolveAccountAcrossCandidates(env, candidates);

    // 未找到账号且设置不接收“无收件人”则直接丢弃
    if (!account && noRecipient === settingConst.noRecipient.CLOSE) {
      return;
    }

    /* ---------- 账号相关风控/权限 ---------- */
    if (account && account.email !== env.admin) {
      let { banEmail, banEmailType, availDomain } =
        await roleService.selectByUserId({ env }, account.userId);

      // 权限校验用最终命中的“规范化地址”finalTo
      if (!roleService.hasAvailDomainPerm(availDomain, finalTo)) {
        return;
      }

      banEmail = (banEmail || '').split(',').filter(item => item !== '');

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

    // 在 email.to 中尽量找与 finalTo/原始 resolvedTo 对应的显示名
    const toName =
      (email.to?.find?.(i => {
        const a = (i.address || '').toLowerCase();
        return a === finalTo || a === normalizeEmail(resolvedTo);
      })?.name) || '';

    /* ---------- 组装入库参数（收件人统一使用 finalTo） ---------- */
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

    /* ---------- 附件处理 ---------- */
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
      if (attachment.contentId) {
        cidAttachments.push(attachment);
      }
    }

    let emailRow = await emailService.receive({ env }, params, cidAttachments, r2Domain);

    attachments.forEach(attachment => {
      attachment.emailId = emailRow.emailId;
      attachment.userId = emailRow.userId;
      attachment.accountId = emailRow.accountId;
    });

    if (attachments.length > 0 && env.r2) {
      await attService.addAtt({ env }, attachments);
    }

    emailRow = await emailService.completeReceive(
      { env },
      account ? emailConst.status.RECEIVE : emailConst.status.NOONE,
      emailRow.emailId
    );

    /* ---------- 可选：按规则限制收件人 ---------- */
    if (ruleType === settingConst.ruleType.RULE) {
      const emails = (ruleEmail || '').split(',').map(e => (e || '').trim().toLowerCase());
      // 同时接受映射后的 finalTo 与原始 resolvedTo
      if (!emails.includes(finalTo) && !emails.includes(normalizeEmail(resolvedTo))) {
        return;
      }
    }

    /* ---------- Telegram 推送 ---------- */
    if (tgBotStatus === settingConst.tgBotStatus.OPEN && tgChatId) {
      const tgMessage = `<b>${params.subject}</b>

<b>发件人：</b>${params.name}\t&lt;${params.sendEmail}&gt;
<b>收件人：\u200B</b>${finalTo}
<b>时间：</b>${dayjs.utc(emailRow.createTime).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm')}

${params.text || emailUtils.htmlToText(params.content) || ''}
`;

      const tgChatIds = tgChatId.split(',');

      await Promise.all(
        tgChatIds.map(async chatId => {
          try {
            const res = await fetch(`https://api.telegram.org/bot${tgBotToken}/sendMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                parse_mode: 'HTML',
                text: tgMessage
              })
            });
            if (!res.ok) {
              console.error(`转发 Telegram 失败: chatId=${chatId}, 状态码=${res.status}`);
            }
          } catch (e) {
            console.error(`转发 Telegram 失败: chatId=${chatId}`, e);
          }
        })
      );
    }

    /* ---------- 邮件再转发 ---------- */
    if (forwardStatus === settingConst.forwardStatus.OPEN && forwardEmail) {
      const emails = forwardEmail.split(',');
      await Promise.all(
        emails.map(async em => {
          try {
            await message.forward(em);
          } catch (e) {
            console.error(`转发邮箱 ${em} 失败：`, e);
          }
        })
      );
    }
  } catch (e) {
    console.error('邮件接收异常: ', e);
  }
}
