const {
  Client,
  GatewayIntentBits,
  Partials,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  EmbedBuilder,
  PermissionFlagsBits,
} = require("discord.js");

const mineflayer = require("mineflayer");
const fs         = require("fs");
const path       = require("path");
const dns        = require("dns");

// Force Google DNS so Railway can resolve game server hostnames
dns.setDefaultResultOrder("ipv4first");
dns.setServers(["8.8.8.8", "8.8.4.4", "1.1.1.1"]);

// Test DNS resolution on startup
dns.lookup("donutsmp.net", (err, addr) => {
  if (err) console.error("❌ DNS lookup failed for donutsmp.net:", err.message);
  else console.log(`✅ DNS resolved donutsmp.net → ${addr}`);
});

// Prevent crashes from interaction timeouts and other unhandled rejections
process.on("unhandledRejection", (err) => {
  if (err?.code === 10062) return; // Unknown interaction — harmless
  console.error("Unhandled rejection:", err?.message || err);
});

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildInvites,
    GatewayIntentBits.DirectMessages,
  ],
  partials: [Partials.Channel, Partials.Message],
});

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const REWARD_LOG_CHANNEL_ID = "1466514242558759278";
const JOIN_LOG_CHANNEL_ID   = "1442916311532441662";
const FRAUD_CHANNEL_ID      = "1442923076147744838";
const REWARD_PER_INVITE     = 5;    // 1 invite = 5m
const MIN_ACCOUNT_AGE_DAYS  = 180;   // account must be this old to count
const BURST_LIMIT           = 5;    // X invites in 60s = invites get reset
const DAILY_LIMIT           = 20;   // 20+ invites in 24h = flagged
const BOT_TOKEN             = process.env.BOT_TOKEN;
const MC_USERNAME           = process.env.MC_USERNAME;
const MC_HOST               = "eu.donutsmp.net";
const MC_PORT               = 25565;
// ──────────────────────────────────────────────────────────────────────────────

// ─── PERSISTENT STORAGE ───────────────────────────────────────────────────────
const DATA_DIR  = process.env.DATA_DIR || "/data";
const DATA_FILE = path.join(DATA_DIR, "data.json");

function ensureDataDir() {
  try { if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true }); }
  catch (e) { console.warn("⚠️ Could not create data dir:", e.message); }
}

function loadData() {
  ensureDataDir();
  try {
    if (fs.existsSync(DATA_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
      console.log("📂 Loaded persistent data from", DATA_FILE);
      return parsed;
    }
  } catch (e) { console.warn("⚠️ Could not load data file:", e.message); }
  return {};
}

function saveData() {
  ensureDataDir();
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify({
      userInvites,
      claimedInvites,
      pendingRewards,
      allTimeInvites,
      bannedUsernames,
      invitedBy,
      joinedBefore: [...joinedBefore],
      rewardListMessageId,
    }, null, 2), "utf8");
  } catch (e) { console.warn("⚠️ Could not save data file:", e.message); }
}

const saved = loadData();

// Current claimable invites per user (resets after payment)
const userInvites    = saved.userInvites    || {};
// Invites already claimed (resets after payment)
const claimedInvites = saved.claimedInvites || {};
// Pending payout list
let pendingRewards   = saved.pendingRewards || {};
// All-time invite leaderboard (NEVER resets)
const allTimeInvites  = saved.allTimeInvites  || {};
// Ban list: { username: { reason, addedBy, addedAt } }
const bannedUsernames = saved.bannedUsernames || {};
// Rejoin detection
const joinedBefore   = new Set(saved.joinedBefore || []);
// Who invited who: { memberId: inviterId }
const invitedBy      = saved.invitedBy || {};
// Reward list message ID
let rewardListMessageId = saved.rewardListMessageId || null;
// ──────────────────────────────────────────────────────────────────────────────

// ─── MINECRAFT BOT STATE ──────────────────────────────────────────────────────
const inviteCache      = new Map();
const inviteTimestamps = {}; // { userId: [timestamp, ...] } for velocity checks
let mcBot             = null;
let mcReady           = false;
let mcConnecting      = false; // prevents multiple simultaneous connection attempts
let payQueue          = [];
let payRunning        = false;
let balPendingChannel = null; // Discord channel waiting for !bal response
// ──────────────────────────────────────────────────────────────────────────────

// ─── READY ────────────────────────────────────────────────────────────────────
client.once("ready", async () => {
  console.log(`✅ ${client.user.tag} is online`);

  if (MC_USERNAME) {
    console.log("🎮 Auto-connecting Minecraft bot...");
    spawnMinecraftBot(null);
  } else {
    console.warn("⚠️ MC_USERNAME not set — Minecraft bot won't connect.");
  }

  for (const guild of client.guilds.cache.values()) {
    try {
      const invites = await guild.invites.fetch();
      const cache = {};
      invites.forEach((inv) => {
        cache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
      });
      inviteCache.set(guild.id, cache);
      const members = await guild.members.fetch();
      members.forEach((m) => joinedBefore.add(m.id));
      console.log(`📋 ${guild.name}: ${invites.size} invites, ${members.size} members cached`);
    } catch (e) { console.error(`Cache error for guild ${guild.id}:`, e.message); }
  }
});

client.on("inviteCreate", (invite) => {
  const cache = inviteCache.get(invite.guild.id) || {};
  cache[invite.code] = { uses: invite.uses, inviterId: invite.inviter?.id };
  inviteCache.set(invite.guild.id, cache);
});

client.on("inviteDelete", (invite) => {
  const cache = inviteCache.get(invite.guild.id) || {};
  delete cache[invite.code];
  inviteCache.set(invite.guild.id, cache);
});

// ─── MEMBER JOIN ──────────────────────────────────────────────────────────────
client.on("guildMemberAdd", async (member) => {
  try {
    const newInvites = await member.guild.invites.fetch();
    const oldCache   = inviteCache.get(member.guild.id) || {};

    const usedInvite = newInvites.find((inv) => {
      const old = oldCache[inv.code];
      return old && inv.uses > old.uses;
    });

    const updatedCache = {};
    newInvites.forEach((inv) => {
      updatedCache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
    });
    inviteCache.set(member.guild.id, updatedCache);

    const joinChannel  = await member.guild.channels.fetch(JOIN_LOG_CHANNEL_ID).catch(() => null);
    const fraudChannel = await member.guild.channels.fetch(FRAUD_CHANNEL_ID).catch(() => null);

    // ── Username ban check ─────────────────────────────────────────────────
    const usernameLower = member.user.username.toLowerCase();
    const banEntry = bannedUsernames[usernameLower];
    if (banEntry) {
      console.log(`🔨 Banned username detected: ${member.user.username} — auto-banning`);
      try {
        await member.send(
          `🔨 **You have been banned from Donut Market.**

` +
          `**Reason:** ${banEntry.reason}

` +
          `If you believe this is a mistake, contact the server staff.`
        ).catch(() => {});
        await member.ban({ reason: `Auto-ban: banned username. Reason: ${banEntry.reason}` });
        if (fraudChannel) {
          await fraudChannel.send({ embeds: [new EmbedBuilder()
            .setColor(0xff0000)
            .setTitle("🔨 Banned Username Auto-Banned")
            .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
            .setDescription(
              `**User:** ${member.user.username} (<@${member.id}>)
` +
              `**Reason:** ${banEntry.reason}
` +
              `**Added by:** ${banEntry.addedBy}
` +
              `**Added at:** <t:${Math.floor(banEntry.addedAt / 1000)}:R>`
            )
            .setTimestamp()] });
        }
      } catch (e) {
        console.error("Failed to auto-ban:", e.message);
        if (fraudChannel) await fraudChannel.send(`❌ Failed to auto-ban **${member.user.username}**: \`${e.message}\``).catch(() => {});
      }
      return;
    }

    const isRejoin     = joinedBefore.has(member.id);
    joinedBefore.add(member.id);

    if (usedInvite && usedInvite.inviter) {
      const inviterId = usedInvite.inviter.id;

      // ── Rejoin ─────────────────────────────────────────────────────────────
      if (isRejoin) {
        console.log(`⚠️ Rejoin: ${member.user.tag} — NOT counted for ${usedInvite.inviter.tag}`);
        if (joinChannel) {
          await joinChannel.send({ embeds: [new EmbedBuilder()
            .setColor(0xed4245)
            .setTitle("🔄 Player Rejoined — Invite Not Counted")
            .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
            .setDescription(`**${member.user.username}** rejoined.\n⚠️ Invite NOT counted for <@${inviterId}> — rejoin detected.`)
            .setFooter({ text: `Members: ${member.guild.memberCount}` })
            .setTimestamp()] });
        }
        return;
      }

      // ── Fraud checks ───────────────────────────────────────────────────────
      const now          = Date.now();
      const fraudReasons = [];

      // 1. Account age
      const ageDays = (now - member.user.createdTimestamp) / 86400000;
      if (ageDays < MIN_ACCOUNT_AGE_DAYS) {
        fraudReasons.push(`Account only **${Math.floor(ageDays)} days old** (min: ${MIN_ACCOUNT_AGE_DAYS} days)`);
      }

      // 2. Velocity tracking
      if (!inviteTimestamps[inviterId]) inviteTimestamps[inviterId] = [];
      inviteTimestamps[inviterId].push(now);
      // Clean old timestamps
      inviteTimestamps[inviterId] = inviteTimestamps[inviterId].filter(t => now - t < 86400000);

      const burst = inviteTimestamps[inviterId].filter(t => now - t < 60000).length;
      const daily = inviteTimestamps[inviterId].length;

      // If 5+ invites in 60 seconds — reset ALL their invites (bot farming)
      if (burst >= BURST_LIMIT) {
        const hadInvites = userInvites[inviterId] || 0;
        userInvites[inviterId]    = 0;
        claimedInvites[inviterId] = 0;
        inviteTimestamps[inviterId] = []; // reset timestamps too
        saveData();

        console.log(`🚨 BURST FRAUD: ${usedInvite.inviter.tag} had ${hadInvites} invites reset — ${burst} invites in 60s`);

        if (fraudChannel) {
          await fraudChannel.send({ embeds: [new EmbedBuilder()
            .setColor(0xff0000)
            .setTitle("🚨 BOT FARMING DETECTED — Invites RESET")
            .setDescription(
              `**Inviter:** <@${inviterId}> (${usedInvite.inviter.tag})\n` +
              `**Invited:** ${member.user.username} (<@${member.id}>)\n\n` +
              `⚠️ **${burst} invites in under 60 seconds** — bot farming detected!\n` +
              `Their invite count has been **reset to 0** (had ${hadInvites} invites).`
            )
            .setFooter({ text: `Account created: ${new Date(member.user.createdTimestamp).toDateString()}` })
            .setTimestamp()] });
        }
        if (joinChannel) {
          await joinChannel.send({ embeds: [new EmbedBuilder()
            .setColor(0xff0000)
            .setTitle("🚨 Bot Farming Detected")
            .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
            .setDescription(`**${member.user.username}** invited by <@${inviterId}>\n🚨 Invites **RESET** — burst farming detected.`)
            .setFooter({ text: `Members: ${member.guild.memberCount}` })
            .setTimestamp()] });
        }
        return;
      }

      // Daily limit check (flag but don't reset automatically)
      if (daily > DAILY_LIMIT) {
        fraudReasons.push(`**${daily} invites in 24 hours** (limit: ${DAILY_LIMIT})`);
      }

      // ── Flagged (account age / daily limit) — don't count ─────────────────
      if (fraudReasons.length > 0) {
        console.log(`🚨 Fraud: ${member.user.tag} by ${usedInvite.inviter.tag} — ${fraudReasons.join(", ")}`);

        if (fraudChannel) {
          await fraudChannel.send({ embeds: [new EmbedBuilder()
            .setColor(0xff6b00)
            .setTitle("⚠️ Suspicious Invite — NOT Counted")
            .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
            .setDescription(
              `**Invited:** ${member.user.username} (<@${member.id}>)\n` +
              `**Inviter:** <@${inviterId}> (${usedInvite.inviter.tag})\n\n` +
              `**Reasons:**\n${fraudReasons.map(r => `> ⚠️ ${r}`).join("\n")}\n\n` +
              `Use \`!addinvites\` to manually credit if legitimate.`
            )
            .setFooter({ text: `Account created: ${new Date(member.user.createdTimestamp).toDateString()}` })
            .setTimestamp()] });
        }
        if (joinChannel) {
          await joinChannel.send({ embeds: [new EmbedBuilder()
            .setColor(0xff6b00)
            .setTitle("⚠️ New Member Joined — Flagged")
            .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
            .setDescription(`**${member.user.username}** was invited by <@${inviterId}>\n⚠️ Invite NOT counted — flagged as suspicious.`)
            .setFooter({ text: `Members: ${member.guild.memberCount}` })
            .setTimestamp()] });
        }
        return;
      }

      // ── Legitimate join — count it ─────────────────────────────────────────
      userInvites[inviterId]    = (userInvites[inviterId]    || 0) + 1;
      allTimeInvites[inviterId] = (allTimeInvites[inviterId] || 0) + 1;
      invitedBy[member.id]      = inviterId; // track who invited this member
      const total = userInvites[inviterId];
      saveData();

      console.log(`📥 ${member.user.tag} joined via ${usedInvite.inviter.tag} — ${total} invite(s) total`);

      if (joinChannel) {
        await joinChannel.send({ embeds: [new EmbedBuilder()
          .setColor(0x57f287)
          .setTitle("👋 New Member Joined!")
          .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
          .setDescription(`**${member.user.username}** was invited by <@${inviterId}>\n<@${inviterId}> now has **${total}** invite${total === 1 ? "" : "s"}.`)
          .setFooter({ text: `Members: ${member.guild.memberCount}` })
          .setTimestamp()] });
      }

    } else {
      // Unknown inviter
      if (!isRejoin && joinChannel) {
        await joinChannel.send({ embeds: [new EmbedBuilder()
          .setColor(0xfee75c)
          .setTitle("👋 New Member Joined!")
          .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
          .setDescription(`**${member.user.username}** joined — inviter couldn't be detected.`)
          .setFooter({ text: `Members: ${member.guild.memberCount}` })
          .setTimestamp()] });
      }
    }
  } catch (e) { console.error("guildMemberAdd error:", e.message); }
});

// ─── MEMBER LEAVE ─────────────────────────────────────────────────────────────
client.on("guildMemberRemove", async (member) => {
  joinedBefore.add(member.id);

  // If we know who invited this member, subtract 1 from their invite count
  const originalInviterId = invitedBy[member.id];
  if (originalInviterId) {
    if (userInvites[originalInviterId] && userInvites[originalInviterId] > 0) {
      userInvites[originalInviterId] = Math.max(0, userInvites[originalInviterId] - 1);
    }
    if (allTimeInvites[originalInviterId] && allTimeInvites[originalInviterId] > 0) {
      allTimeInvites[originalInviterId] = Math.max(0, allTimeInvites[originalInviterId] - 1);
    }
    delete invitedBy[member.id]; // clean up
    console.log(`📤 ${member.user.tag} left — removed 1 invite from inviter ${originalInviterId}`);
  }

  saveData();

  try {
    const newInvites = await member.guild.invites.fetch();
    const updatedCache = {};
    newInvites.forEach((inv) => { updatedCache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id }; });
    inviteCache.set(member.guild.id, updatedCache);
  } catch (e) { console.error("guildMemberRemove error:", e.message); }
});

// ─── COMMANDS ─────────────────────────────────────────────────────────────────
client.on("messageCreate", async (message) => {
  if (message.author.bot) return;

  const isAdmin =
    message.member?.permissions.has(PermissionFlagsBits.Administrator) ||
    message.guild?.ownerId === message.author.id;

  const args    = message.content.trim().split(/\s+/);
  const command = args[0].toLowerCase();

  // ── !reward ────────────────────────────────────────────────────────────────
  if (command === "!reward") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    await message.delete().catch(() => {});
    await message.channel.send({
      embeds: [new EmbedBuilder()
        .setColor(0x00c8ff)
        .setTitle("🎁 Invite Rewards")
        .setDescription(
          `Have you invited people to the server? Click **Claim Rewards** below!\n\n` +
          `**Rate:** 1 invite = ${REWARD_PER_INVITE}m\n\n` +
          `Your invite count is checked **automatically** — just enter your Minecraft IGN.`
        )
        .setFooter({ text: "Invite Rewards System" })],
      components: [new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId("claim_reward").setLabel("🎁 Claim Rewards").setStyle(ButtonStyle.Primary)
      )],
    });
    return;
  }

  // ── !invboard — post the invite reward queue ───────────────────────────────
  if (command === "!invboard") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    await message.delete().catch(() => {});
    // Force a fresh post of the reward list
    rewardListMessageId = null;
    await updateRewardList(client, message.guild);
    return;
  }

  // ── !invites @user ─────────────────────────────────────────────────────────
  if (command === "!invites") {
    const mention = message.mentions.users.first() || message.author;
    const total     = userInvites[mention.id]    || 0;
    const claimed   = claimedInvites[mention.id] || 0;
    const allTime   = allTimeInvites[mention.id] || 0;
    const available = total - claimed;

    await message.reply({ embeds: [new EmbedBuilder()
      .setColor(0x00c8ff)
      .setTitle(`📊 Invites — ${mention.username}`)
      .addFields(
        { name: "Available to claim", value: `**${available}** (${available * REWARD_PER_INVITE}m)`, inline: true },
        { name: "Total (this cycle)",  value: `**${total}**`,   inline: true },
        { name: "All-time invites",    value: `**${allTime}**`, inline: true },
      )
      .setThumbnail(mention.displayAvatarURL({ dynamic: true }))
      .setTimestamp()] });
    return;
  }

  // ── !resetinvites @user ────────────────────────────────────────────────────
  if (command === "!resetinvites") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    const mention = message.mentions.users.first();
    if (!mention) {
      const r = await message.reply("❌ Usage: `!resetinvites @user`");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    userInvites[mention.id]    = 0;
    claimedInvites[mention.id] = 0;
    // Also remove from pending queue if they're in it
    if (pendingRewards[mention.id]) {
      delete pendingRewards[mention.id];
      await updateRewardList(client, message.guild);
    }
    saveData();
    await message.reply(`✅ Invite count for <@${mention.id}> has been reset to 0.`);
    return;
  }

  // ── !topinvites — all-time leaderboard ────────────────────────────────────
  if (command === "!topinvites") {
    const sorted = Object.entries(allTimeInvites)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 10);

    if (sorted.length === 0) {
      await message.reply("📊 No invite data yet!");
      return;
    }

    const medals = ["🥇", "🥈", "🥉"];
    const lines  = sorted.map(([uid, count], i) =>
      `${medals[i] || `**${i + 1}.**`} <@${uid}> — **${count}** invite${count === 1 ? "" : "s"} (${count * REWARD_PER_INVITE}m total earned)`
    );

    await message.reply({ embeds: [new EmbedBuilder()
      .setColor(0xf5a623)
      .setTitle("🏆 Top Inviters — All Time")
      .setDescription(lines.join("\n"))
      .setFooter({ text: "All-time leaderboard — never resets" })
      .setTimestamp()] });
    return;
  }

  // ── !addinvites @user amount ───────────────────────────────────────────────
  if (command === "!addinvites") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    const mention = message.mentions.users.first();
    const amount  = parseInt(args[2]);
    if (!mention || isNaN(amount) || amount <= 0) {
      const r = await message.reply("❌ Usage: `!addinvites @user <amount>`");
      setTimeout(() => r.delete().catch(() => {}), 8000);
      return;
    }
    userInvites[mention.id]    = (userInvites[mention.id]    || 0) + amount;
    allTimeInvites[mention.id] = (allTimeInvites[mention.id] || 0) + amount;
    saveData();
    await message.reply(`✅ Added **${amount}** invite${amount === 1 ? "" : "s"} to <@${mention.id}>. They now have **${userInvites[mention.id]}** invites.`);
    return;
  }

  // ── !removeinvites @user amount ───────────────────────────────────────────
  if (command === "!removeinvites") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    const mention = message.mentions.users.first();
    const amount  = parseInt(args[2]);
    if (!mention || isNaN(amount) || amount <= 0) {
      const r = await message.reply("❌ Usage: `!removeinvites @user <amount>`");
      setTimeout(() => r.delete().catch(() => {}), 8000);
      return;
    }
    userInvites[mention.id] = Math.max(0, (userInvites[mention.id] || 0) - amount);
    saveData();
    await message.reply(`✅ Removed **${amount}** invite${amount === 1 ? "" : "s"} from <@${mention.id}>. They now have **${userInvites[mention.id]}** invites.`);
    return;
  }

  // ── !checkinvites @user ────────────────────────────────────────────────────
  if (command === "!checkinvites") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    const mention = message.mentions.users.first() || message.author;
    const total   = userInvites[mention.id]    || 0;
    const claimed = claimedInvites[mention.id] || 0;
    await message.reply(`📊 **${mention.username}** — Total: **${total}** | Claimed: **${claimed}** | Available: **${total - claimed}**`);
    return;
  }

  // ── !banlist — open the ban management GUI ───────────────────────────────
  if (command === "!banlist") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }

    const entries = Object.entries(bannedUsernames);
    const embed = new EmbedBuilder()
      .setColor(0xff0000)
      .setTitle("🔨 Username Ban List")
      .setDescription(entries.length === 0
        ? "No usernames banned yet. Use `!addban` to add one."
        : entries.map(([name, data], i) => `**${i + 1}.** \`${name}\` — ${data.reason} *(by ${data.addedBy})*`).join("\n")
      )
      .setFooter({ text: `${entries.length} banned username(s)` })
      .setTimestamp();

    const components = [];
    if (entries.length > 0) {
      const chunks = chunkArray(entries.slice(0, 20), 4);
      for (const chunk of chunks) {
        components.push(new ActionRowBuilder().addComponents(
          chunk.map(([name]) =>
            new ButtonBuilder()
              .setCustomId(`unban_username_${name}`)
              .setLabel(`🗑️ ${name}`)
              .setStyle(ButtonStyle.Danger)
          )
        ));
      }
    }

    // Add ban button
    components.push(new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId("add_username_ban")
        .setLabel("➕ Add Username Ban")
        .setStyle(ButtonStyle.Primary)
    ));

    await message.reply({ embeds: [embed], components });
    return;
  }

  // ── !addban <username> <reason> ────────────────────────────────────────────
  if (command === "!addban") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    const username = args[1]?.toLowerCase();
    const reason   = args.slice(2).join(" ") || "Ban evasion";
    if (!username) {
      const r = await message.reply("❌ Usage: `!addban <minecraft_ign> <reason>`");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    bannedUsernames[username] = { reason, addedBy: message.author.tag, addedAt: Date.now() };
    saveData();
    await message.reply(`✅ Added \`${username}\` to the ban list. Reason: **${reason}**`);
    return;
  }

  // ── !removeban <username> ──────────────────────────────────────────────────
  if (command === "!removeban") {
    if (!isAdmin) {
      const r = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    const username = args[1]?.toLowerCase();
    if (!username || !bannedUsernames[username]) {
      const r = await message.reply(`❌ Username \`${username}\` not found in ban list.`);
      setTimeout(() => r.delete().catch(() => {}), 5000);
      return;
    }
    delete bannedUsernames[username];
    saveData();
    await message.reply(`✅ Removed \`${username}\` from the ban list.`);
    return;
  }

  // ── !bal — check the MC bot's in-game balance ────────────────────────────
  if (command === "!bal") {
    if (!isAdmin) return;
    if (!mcBot || !mcReady) {
      await message.reply("❌ Minecraft bot is not connected.");
      return;
    }
    balPendingChannel = message.channel;
    mcBot.chat("/bal");
    await message.reply("⏳ Checking balance... response will appear here shortly.");
    // Auto-clear after 10s if no response came
    setTimeout(() => {
      if (balPendingChannel === message.channel) {
        balPendingChannel = null;
      }
    }, 10000);
    return;
  }

  // ── !clearauth — delete cached Microsoft token and force re-login ──────────
  if (command === "!clearauth") {
    if (!isAdmin) return;

    const authCacheDir = path.join(DATA_DIR, "auth_cache");
    try {
      if (fs.existsSync(authCacheDir)) {
        const files = fs.readdirSync(authCacheDir);
        for (const file of files) {
          fs.unlinkSync(path.join(authCacheDir, file));
        }
        await message.reply("✅ Auth cache cleared! Reconnecting now — check this channel for the verification code.");
      } else {
        await message.reply("ℹ️ No auth cache found. Reconnecting anyway...");
      }
    } catch (e) {
      await message.reply(`❌ Failed to clear auth cache: \`${e.message}\``);
      return;
    }

    // Disconnect existing bot and reconnect with fresh auth
    if (mcBot) { mcBot.quit(); mcBot = null; mcReady = false; }
    await spawnMinecraftBot(message.channel);
    return;
  }

  // ── !mcreconnect ───────────────────────────────────────────────────────────
  if (command === "!mcreconnect") {
    if (!isAdmin) return;
    await message.delete().catch(() => {});
    await spawnMinecraftBot(message.channel);
    return;
  }

  // ── !mcstatus ──────────────────────────────────────────────────────────────
  if (command === "!mcstatus") {
    if (!isAdmin) return;
    await message.reply((!mcBot || !mcReady)
      ? "🔴 Minecraft bot is **not connected**."
      : `🟢 Minecraft bot is **online** as \`${mcBot.username}\` on \`${MC_HOST}:${MC_PORT}\``);
    return;
  }

  // ── !mckick ────────────────────────────────────────────────────────────────
  if (command === "!mckick") {
    if (!isAdmin) return;
    if (mcBot) {
      mcBot.quit(); mcBot = null; mcReady = false;
      await message.reply("✅ Minecraft bot disconnected.");
    } else {
      await message.reply("❌ No Minecraft bot connected.");
    }
    return;
  }

  // ── !cd <command> — send a command to Minecraft as the bot ─────────────────
  if (command === "!cd") {
    if (!isAdmin) return;

    const mcCommand = args.slice(1).join(" ");

    if (!mcCommand) {
      await message.reply("❌ Usage: `!cd <command>`\nExample: `!cd tpa Steve` or `!cd say Hello!`");
      return;
    }

    if (!mcBot || !mcReady) {
      await message.reply("❌ Minecraft bot is not connected. Use `!mcreconnect` first.");
      return;
    }

    try {
      // Prefix with / if not already there
      const toSend = mcCommand.startsWith("/") ? mcCommand : `/${mcCommand}`;
      mcBot.chat(toSend);
      await message.reply(`✅ Sent to Minecraft: \`${toSend}\``);
    } catch (e) {
      await message.reply(`❌ Failed to send command: \`${e.message}\``);
    }
    return;
  }
});

// ─── Spawn the Mineflayer bot ─────────────────────────────────────────────────
async function spawnMinecraftBot(feedbackChannel) {
  // Prevent multiple simultaneous connection attempts
  if (mcConnecting) {
    console.log("⚠️ Already connecting — ignoring duplicate spawn request");
    return;
  }
  mcConnecting = true;
  if (mcBot) { try { mcBot.quit(); } catch {} mcBot = null; mcReady = false; }

  console.log(`🔄 Attempting to connect to ${MC_HOST}:${MC_PORT} as ${MC_USERNAME}...`);
  if (feedbackChannel) await feedbackChannel.send("🔄 Connecting Minecraft bot...");

  const authCacheDir = path.join(DATA_DIR, "auth_cache");
  try { if (!fs.existsSync(authCacheDir)) fs.mkdirSync(authCacheDir, { recursive: true }); }
  catch (e) { console.warn("Could not create auth cache dir:", e.message); }

  try {
    mcBot = mineflayer.createBot({
      host:           MC_HOST,
      port:           MC_PORT,
      username:       MC_USERNAME,
      auth:           "microsoft",
      version:        "1.20.5", // updated version
      profilesFolder: authCacheDir,
      keepAlive:      true,
    });

    console.log("✅ mineflayer.createBot() called — waiting for connection...");

    // Timeout: if bot hasn't spawned within 2 minutes, force reconnect
    const spawnTimeout = setTimeout(async () => {
      if (!mcReady && mcBot) {
        console.log("⚠️ Bot took too long to spawn — forcing reconnect");
        try { mcBot.quit(); } catch {}
        mcBot        = null;
        mcConnecting = false;
        const ch = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (ch) await ch.send("⚠️ Minecraft bot took too long to connect — retrying in 30s...").catch(() => {});
        setTimeout(() => spawnMinecraftBot(null), 30000);
      }
    }, 120000);

    // Clear timeout once spawned
    mcBot.once("spawn", () => clearTimeout(spawnTimeout));

    mcBot.on("microsoftDeviceCode", async (data) => {
      const msg =
        `🔐 **One-time Microsoft Verification**\n\n` +
        `1. Go to: **${data.verificationUri}**\n` +
        `2. Enter code: \`${data.userCode}\`\n` +
        `3. Sign in with the Microsoft account linked to your Minecraft Java account\n\n` +
        `✅ After this you'll **never need to verify again** — the token is saved permanently!`;
      console.log(`🔐 Microsoft verification — code: ${data.userCode} at ${data.verificationUri}`);
      if (feedbackChannel) { await feedbackChannel.send(msg); }
      else {
        const ch = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (ch) await ch.send(msg);
      }
    });

    mcBot.once("spawn", async () => {
      mcReady      = true;
      mcConnecting = false; // connection succeeded
      console.log(`🎮 Minecraft bot spawned as ${mcBot.username} on ${MC_HOST}:${MC_PORT}`);
      const msg = `✅ Minecraft bot **${mcBot.username}** is online on **${MC_HOST}** and ready to pay rewards!`;
      if (feedbackChannel) { await feedbackChannel.send(msg); }
      else {
        const ch = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (ch) await ch.send(msg);
      }
      setTimeout(() => { if (mcBot && mcReady) mcBot.look(mcBot.entity.yaw + 0.3, mcBot.entity.pitch, false); }, 2000);
      setTimeout(() => { if (mcBot && mcReady) mcBot.look(mcBot.entity.yaw - 0.1, 0, false); }, 5000);
      if (payQueue.length > 0) { console.log(`⏳ Processing ${payQueue.length} queued payment(s)...`); runPayQueue(); }
    });

    // Listen to all chat messages from the Minecraft server
    mcBot.on("message", async (jsonMsg) => {
      const text = jsonMsg.toString().trim();
      if (!text) return;
      console.log(`[MC Chat] ${text}`);

      // Forward balance response to Discord when !bal was used
      if (balPendingChannel) {
        // Most economy plugins respond with something containing the balance
        // Common formats: "Your balance: 1,234m", "Balance: $1234", etc.
        // Format: "You have $1B"
        const isBalResponse = /you have \$[\d\.]+[kmbt]?/i.test(text);
        if (isBalResponse) {
          await balPendingChannel.send(`💰 **MC Bot Balance:**
\`\`\`${text}\`\`\``).catch(() => {});
          balPendingChannel = null;
        }
      }

      // Log payment confirmations — forward to reward log channel
      // Format: "You paid StevenIsBlack $5M." or "StevenIsBlack paid you $5M."
      const isPayConfirm = /you paid .+ \$[\d\.]+[kmbt]?/i.test(text) || /paid you \$[\d\.]+[kmbt]?/i.test(text);
      if (isPayConfirm) {
        const logCh = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (logCh) await logCh.send(`✅ **MC Confirmation:** \`${text}\``).catch(() => {});
      }
    });

    mcBot.on("kicked", async (reason) => {
      mcReady = false;
      let r = reason;
      try { const p = typeof reason === "string" ? JSON.parse(reason) : reason; r = p?.text || p?.translate || JSON.stringify(p); }
      catch { r = String(reason); }
      console.log(`⚠️ Minecraft bot was kicked: ${r}`);
      const ch = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
      if (ch) await ch.send(`⚠️ Minecraft bot was **kicked**: \`${r}\` — reconnecting in 60s...`);
      mcBot = null;
      setTimeout(() => spawnMinecraftBot(null), 60000);
    });

    mcBot.on("error", async (err) => {
      mcReady      = false;
      mcConnecting = false;
      mcBot        = null;
      console.error("Minecraft bot error:", err.message);
      const ch = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);

      // If account is suspended, don't retry — alert admin
      if (err.message.includes("ACCOUNT_SUSPENDED") || err.message.includes("suspended")) {
        console.error("🚨 Microsoft account suspended — not retrying. Get a new Minecraft account.");
        if (ch) await ch.send(
          `🚨 **Minecraft account suspended by Microsoft!**

` +
          `The account **${MC_USERNAME}** has been suspended — this is why the bot stopped working.

` +
          `**Fix:** Get a new Minecraft Java account, update \`MC_USERNAME\` in Railway variables, then type \`!clearauth\`.`
        ).catch(() => {});
        return; // Don't reconnect
      }

      // If rate limited, wait longer before retrying
      const retryDelay = err.message.includes("429") ? 300000 : 30000; // 5 min if rate limited
      if (ch) await ch.send(`❌ Minecraft bot error: \`${err.message}\` — reconnecting in ${retryDelay / 1000}s...`).catch(() => {});
      setTimeout(() => spawnMinecraftBot(null), retryDelay);
    });

    mcBot.on("end", async () => {
      const wasReady = mcReady;
      mcReady      = false;
      mcConnecting = false;
      mcBot        = null;
      if (wasReady) {
        console.log("Minecraft bot disconnected — reconnecting in 90s...");
        const ch = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (ch) await ch.send(`⚠️ Minecraft bot disconnected — reconnecting in 90s...`).catch(() => {});
      } else {
        console.log("Minecraft bot ended before spawning — reconnecting in 90s...");
        const ch = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (ch) await ch.send(`⚠️ Minecraft bot failed to connect — reconnecting in 90s...`).catch(() => {});
      }
      setTimeout(() => spawnMinecraftBot(null), 90000);
    });

  } catch (e) {
    mcConnecting = false;
    console.error("Failed to start Minecraft bot:", e.message);
    if (feedbackChannel) await feedbackChannel.send(`❌ Failed to start Minecraft bot: \`${e.message}\``);
  }
}

// ─── Run pay queue ─────────────────────────────────────────────────────────────
async function runPayQueue() {
  // Prevent multiple simultaneous runs
  if (payRunning) return;
  if (payQueue.length === 0) return;

  payRunning = true;

  // Snapshot and clear the queue atomically so nothing gets added mid-run
  const toProcess = [...payQueue];
  payQueue = [];

  // Merge duplicate IGNs so shared accounts get one combined payment
  const merged = {};
  for (const { ign, amount } of toProcess) {
    const key = ign.toLowerCase();
    merged[key] = { ign, amount: (merged[key]?.amount || 0) + amount };
  }

  const entries = Object.values(merged);
  console.log(`💰 Starting pay queue: ${entries.length} payment(s) to send`);

  const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);

  for (let i = 0; i < entries.length; i++) {
    const { ign, amount } = entries[i];

    if (!mcBot || !mcReady) {
      // Bot went offline mid-queue — re-add ALL remaining entries and stop
      const remaining = entries.slice(i);
      for (const entry of remaining) payQueue.push(entry);
      console.log(`⚠️ MC bot went offline — re-queued ${remaining.length} payment(s)`);
      if (logChannel) await logChannel.send(`⚠️ MC bot went offline mid-payment — **${remaining.length}** payment(s) re-queued. They will be sent when the bot reconnects.`).catch(() => {});
      break;
    }

    const cmd = `/pay ${ign} ${amount}m`;
    try {
      mcBot.chat(cmd);
      console.log(`💰 [${i + 1}/${entries.length}] Sent: ${cmd}`);
    } catch (e) {
      // Command failed — re-queue this specific payment
      payQueue.push({ ign, amount });
      console.error(`❌ Failed to send ${cmd} — re-queued:`, e.message);
      if (logChannel) await logChannel.send(`❌ Failed to send \`${cmd}\` — re-queued.`).catch(() => {});
    }

    // 2 second gap between commands — more reliable than 1.5s
    await new Promise((r) => setTimeout(r, 2000));
  }

  console.log(`✅ Pay queue finished`);
  payRunning = false;

  // If new entries were added while we were running, process them now
  if (payQueue.length > 0) {
    console.log(`⏳ ${payQueue.length} new payment(s) queued during run — processing now`);
    runPayQueue();
  }
}

// ─── INTERACTIONS ─────────────────────────────────────────────────────────────
client.on("interactionCreate", async (interaction) => {

  // ── Claim button → modal ───────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "claim_reward") {
    const modal = new ModalBuilder().setCustomId("reward_modal").setTitle("Claim Your Invite Rewards");
    modal.addComponents(new ActionRowBuilder().addComponents(
      new TextInputBuilder().setCustomId("ign").setLabel("Your Minecraft Username")
        .setStyle(TextInputStyle.Short).setPlaceholder("e.g. Steve").setRequired(true)
    ));
    await interaction.showModal(modal);
    return;
  }

  // ── Modal submit ───────────────────────────────────────────────────────────
  if (interaction.isModalSubmit() && interaction.customId === "reward_modal") {
    await interaction.deferReply({ ephemeral: true });

    const ign        = interaction.fields.getTextInputValue("ign").trim();
    const userId     = interaction.user.id;
    const discordTag = interaction.user.tag;
    const total      = userInvites[userId]    || 0;
    const claimed    = claimedInvites[userId] || 0;
    const claimable  = total - claimed;

    // ── Banned IGN check ───────────────────────────────────────────────────
    const ignLower  = ign.toLowerCase();
    const ignBanEntry = bannedUsernames[ignLower];
    if (ignBanEntry) {
      console.log(`🔨 Banned IGN used by ${discordTag} (${userId}): ${ign}`);

      // Tell them before banning so the DM goes through
      await interaction.editReply({ content: `🔨 **You have been banned.**

**Reason:** ${ignBanEntry.reason}` });

      // DM them
      try {
        await interaction.user.send(
          `🔨 **You have been banned from Donut Market.**

` +
          `**Reason:** ${ignBanEntry.reason}

` +
          `If you believe this is a mistake, contact the server staff.`
        );
      } catch { /* DMs off, ignore */ }

      // Ban them
      try {
        const member = await interaction.guild.members.fetch(userId);
        await member.ban({ reason: `Banned IGN used: ${ign}. Reason: ${ignBanEntry.reason}` });
      } catch (e) {
        console.error("Failed to ban member:", e.message);
      }

      // Alert in fraud channel
      const fraudCh = await interaction.client.channels.fetch(FRAUD_CHANNEL_ID).catch(() => null);
      if (fraudCh) {
        await fraudCh.send({ embeds: [new EmbedBuilder()
          .setColor(0xff0000)
          .setTitle("🔨 Banned IGN — User Banned")
          .setDescription(
            `**Discord:** ${discordTag} (<@${userId}>)
` +
            `**Minecraft IGN entered:** \`${ign}\`
` +
            `**Reason:** ${ignBanEntry.reason}
` +
            `**Ban added by:** ${ignBanEntry.addedBy}`
          )
          .setTimestamp()] }).catch(() => {});
      }
      return;
    }

    // ── Minimum server membership check (24 hours) ───────────────────────────
    const member = await interaction.guild.members.fetch(userId).catch(() => null);
    if (member) {
      const joinedAt   = member.joinedTimestamp;
      const hoursInServer = (Date.now() - joinedAt) / (1000 * 60 * 60);
      if (hoursInServer < 24) {
        const hoursLeft = Math.ceil(24 - hoursInServer);
        await interaction.editReply({
          content:
            `⏳ **You need to be in the server for at least 24 hours before claiming rewards.**

` +
            `You joined **${Math.floor(hoursInServer)} hour${Math.floor(hoursInServer) === 1 ? "" : "s"}** ago.
` +
            `Come back in **${hoursLeft} hour${hoursLeft === 1 ? "" : "s"}**!`,
        });
        return;
      }
    }

    if (pendingRewards[userId]) {
      const p = pendingRewards[userId];
      await interaction.editReply({ content: `⏳ You already have a pending claim!\n\n**IGN:** ${p.ign} | **Invites:** ${p.invites} | **Reward:** ${p.invites * REWARD_PER_INVITE}m\n\nWait for it to be paid before claiming again.` });
      return;
    }
    if (claimable <= 0) {
      await interaction.editReply({ content: `❌ You have no unclaimed invites.\n\n📊 Total: **${total}** | Claimed: **${claimed}**\n\nInvite more players to earn rewards!` });
      return;
    }

    pendingRewards[userId] = { ign, invites: claimable, discordTag };
    saveData();
    await updateRewardList(interaction.client, interaction.guild);
    await interaction.editReply({ content: `✅ **Claim submitted!**\n\n🎮 **IGN:** ${ign}\n🎟️ **Invites:** ${claimable}\n💰 **Reward:** ${claimable * REWARD_PER_INVITE}m\n\nYou'll get a DM when paid in-game! 🎉` });
    return;
  }

  // ── Add username ban modal ────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "add_username_ban") {
    if (!interaction.member.permissions.has(PermissionFlagsBits.Administrator) && interaction.guild.ownerId !== interaction.user.id) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }
    const modal = new ModalBuilder().setCustomId("add_ban_modal").setTitle("Ban a Minecraft IGN");
    modal.addComponents(
      new ActionRowBuilder().addComponents(
        new TextInputBuilder().setCustomId("ban_username").setLabel("Minecraft IGN (exact, case-insensitive)")
          .setStyle(TextInputStyle.Short).setPlaceholder("e.g. Steve123").setRequired(true)
      ),
      new ActionRowBuilder().addComponents(
        new TextInputBuilder().setCustomId("ban_reason").setLabel("Reason")
          .setStyle(TextInputStyle.Short).setPlaceholder("e.g. Ban evasion - original account: steve").setRequired(true)
      )
    );
    await interaction.showModal(modal);
    return;
  }

  // ── Add ban modal submit ───────────────────────────────────────────────────
  if (interaction.isModalSubmit() && interaction.customId === "add_ban_modal") {
    await interaction.deferReply({ ephemeral: true });
    const username = interaction.fields.getTextInputValue("ban_username").trim().toLowerCase();
    const reason   = interaction.fields.getTextInputValue("ban_reason").trim();

    bannedUsernames[username] = { reason, addedBy: interaction.user.tag, addedAt: Date.now() };
    saveData();

    await interaction.editReply({ content: `✅ Added \`${username}\` to the ban list.
**Reason:** ${reason}

If they try to join the server they will be instantly banned and DM'd the reason.` });
    return;
  }

  // ── Unban username button ──────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId.startsWith("unban_username_")) {
    if (!interaction.member.permissions.has(PermissionFlagsBits.Administrator) && interaction.guild.ownerId !== interaction.user.id) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    const username = interaction.customId.replace("unban_username_", "");
    if (!bannedUsernames[username]) {
      await interaction.editReply({ content: `❌ \`${username}\` not found in ban list.` });
      return;
    }
    delete bannedUsernames[username];
    saveData();
    await interaction.editReply({ content: `✅ Removed \`${username}\` from the ban list.` });
    return;
  }

  // ── Remove from queue button ───────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId.startsWith("remove_queue_")) {
    if (!interaction.member.permissions.has(PermissionFlagsBits.Administrator) && interaction.guild.ownerId !== interaction.user.id) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    const targetId = interaction.customId.replace("remove_queue_", "");
    if (!pendingRewards[targetId]) {
      await interaction.editReply({ content: "❌ Not found in pending list." });
      return;
    }
    const data = pendingRewards[targetId];
    delete pendingRewards[targetId];
    saveData();
    await updateRewardList(interaction.client, interaction.guild);
    await interaction.editReply({ content: `🗑️ Removed **${data.ign}** (${data.discordTag}) from the queue without paying.` });
    return;
  }

  // ── Mark individual paid ───────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId.startsWith("mark_paid_")) {
    if (!interaction.member.permissions.has(PermissionFlagsBits.Administrator) && interaction.guild.ownerId !== interaction.user.id) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    const targetId = interaction.customId.replace("mark_paid_", "");
    if (!pendingRewards[targetId]) {
      await interaction.editReply({ content: "❌ Not found in pending list." });
      return;
    }
    const data   = pendingRewards[targetId];
    const amount = data.invites * REWARD_PER_INVITE;
    payQueue.push({ ign: data.ign, amount });
    await payUser(interaction.client, interaction.guild, targetId);
    await updateRewardList(interaction.client, interaction.guild);
    if (mcBot && mcReady) runPayQueue();
    const status = (!mcBot || !mcReady) ? `⚠️ MC bot offline — payment queued.` : `✅ Paying **${data.ign}** ${amount}m in-game now!`;
    await interaction.editReply({ content: status });
    return;
  }

  // ── Pay all ────────────────────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "pay_all") {
    if (!interaction.member.permissions.has(PermissionFlagsBits.Administrator) && interaction.guild.ownerId !== interaction.user.id) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    const all = Object.keys(pendingRewards);
    if (all.length === 0) { await interaction.editReply({ content: "✅ Nothing pending!" }); return; }

    const snapshot = all.map(uid => ({ uid, ign: pendingRewards[uid].ign, amount: pendingRewards[uid].invites * REWARD_PER_INVITE }));
    for (const { ign, amount } of snapshot) payQueue.push({ ign, amount });
    for (const uid of all) await payUser(interaction.client, interaction.guild, uid);
    await updateRewardList(interaction.client, interaction.guild);
    if (mcBot && mcReady) runPayQueue();

    const status = (!mcBot || !mcReady)
      ? `✅ ${snapshot.length} payment(s) queued — will be sent when MC bot reconnects.`
      : `✅ Paying ${snapshot.length} player(s) in-game now!`;
    await interaction.editReply({ content: status });
    return;
  }
});

// ─── Pay one user (DM + clear pending) ────────────────────────────────────────
async function payUser(client, guild, userId) {
  const data = pendingRewards[userId];
  if (!data) return;
  userInvites[userId]    = 0;
  claimedInvites[userId] = 0;
  delete pendingRewards[userId];
  saveData();
  try {
    const member = await guild.members.fetch(userId);
    await member.send(
      `✅ **Your invite rewards are being paid in-game!**\n\n` +
      `🎮 **IGN:** ${data.ign}\n🎟️ **Invites:** ${data.invites}\n💰 **Amount:** ${data.invites * REWARD_PER_INVITE}m\n\n` +
      `Please leave a vouch in **#Vouches-rewards**! 🎉`
    );
  } catch { console.log(`⚠️ Could not DM ${userId}`); }
}

// ─── Build / update the payout list ──────────────────────────────────────────
async function updateRewardList(client, guild) {
  const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
  if (!logChannel) return console.error("❌ Reward log channel not found.");

  const unpaid   = Object.entries(pendingRewards);
  const mcStatus = (!mcBot || !mcReady) ? "🔴 MC Bot Offline — auto-reconnecting..." : `🟢 MC Bot Online as \`${mcBot.username}\``;

  const embed = new EmbedBuilder()
    .setColor(0xf5a623)
    .setTitle("🎁 Invite Reward Queue")
    .setTimestamp()
    .setFooter({ text: mcStatus });

  if (unpaid.length === 0) {
    embed.setDescription("✅ Queue is empty — all paid!");
  } else {
    const lines = unpaid.map(([, data], i) => {
      const reward = data.invites * REWARD_PER_INVITE;
      return `**${i + 1}.** ${data.discordTag} · IGN: \`${data.ign}\` · **${data.invites}** invite${data.invites === 1 ? "" : "s"} → **${reward}m**`;
    });
    const totalM = unpaid.reduce((s, [, d]) => s + d.invites * REWARD_PER_INVITE, 0);
    embed.setDescription(lines.join("\n"));
    embed.addFields({ name: "Total to pay", value: `**${totalM}m**`, inline: false });
  }

  const components = [];

  if (unpaid.length > 0) {
    // Pay buttons (green) — up to 10 people shown (2 rows of 5 to leave room for remove buttons)
    const payChunks = chunkArray(unpaid.slice(0, 10), 5);
    for (const chunk of payChunks) {
      components.push(new ActionRowBuilder().addComponents(
        chunk.map(([uid, data]) =>
          new ButtonBuilder().setCustomId(`mark_paid_${uid}`).setLabel(`✅ ${data.ign}`).setStyle(ButtonStyle.Success)
        )
      ));
    }

    // Remove buttons (red) — matching the same people
    const removeChunks = chunkArray(unpaid.slice(0, 10), 5);
    for (const chunk of removeChunks) {
      components.push(new ActionRowBuilder().addComponents(
        chunk.map(([uid, data]) =>
          new ButtonBuilder().setCustomId(`remove_queue_${uid}`).setLabel(`🗑️ ${data.ign}`).setStyle(ButtonStyle.Danger)
        )
      ));
    }

    // Pay All button
    components.push(new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("pay_all").setLabel("💰 Pay All & Clear Queue").setStyle(ButtonStyle.Primary)
    ));
  }

  if (rewardListMessageId) {
    try {
      const existing = await logChannel.messages.fetch(rewardListMessageId);
      await existing.edit({ embeds: [embed], components });
      return;
    } catch { /* message deleted, send new */ }
  }

  const sent = await logChannel.send({ embeds: [embed], components });
  rewardListMessageId = sent.id;
  saveData();
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

client.login(BOT_TOKEN).catch(err => {
  console.error("LOGIN ERROR:", err);
});
