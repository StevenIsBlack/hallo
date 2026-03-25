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
const REWARD_PER_INVITE     = 5; // 1 invite = 5m
const BOT_TOKEN             = process.env.BOT_TOKEN;

// Minecraft bot credentials — set MC_USERNAME in Railway environment variables
// MC_USERNAME = your Minecraft Java username (NOT email, just the in-game name e.g. "Steve")
const MC_USERNAME = process.env.MC_USERNAME;
const MC_HOST     = "donutsmp.net";
const MC_PORT     = 25565;
// ──────────────────────────────────────────────────────────────────────────────

// ─── MINECRAFT BOT STATE ──────────────────────────────────────────────────────
let mcBot   = null;  // the active mineflayer bot instance
let mcReady = false; // true once the bot has spawned in-game

// Queue of pay commands to run: [{ ign, amount }]
let payQueue     = [];
let payRunning   = false;
// ──────────────────────────────────────────────────────────────────────────────

// invite cache: guildId → { code: { uses, inviterId } }
const inviteCache = new Map();

// total invites per user: { userId: number }
const userInvites = {};

// invites already paid out: { userId: number }
const claimedInvites = {};

// pending payout list: { userId: { ign, invites, discordTag } }
let pendingRewards = {};

// everyone who has ever been in the server (rejoin detection)
const joinedBefore = new Set();

// ID of the live payout-list message in REWARD_LOG_CHANNEL
let rewardListMessageId = null;

// ─── READY ────────────────────────────────────────────────────────────────────
client.once("ready", async () => {
  console.log(`✅ ${client.user.tag} is online`);

  // Auto-connect Minecraft bot on startup
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
    } catch (e) {
      console.error(`Cache error for guild ${guild.id}:`, e.message);
    }
  }
});

// ─── INVITE CREATED / DELETED ─────────────────────────────────────────────────
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

    const joinChannel = await member.guild.channels.fetch(JOIN_LOG_CHANNEL_ID).catch(() => null);
    const isRejoin    = joinedBefore.has(member.id);
    joinedBefore.add(member.id);

    if (usedInvite && usedInvite.inviter) {
      const inviterId = usedInvite.inviter.id;

      if (isRejoin) {
        console.log(`⚠️ Rejoin: ${member.user.tag} — NOT counted for ${usedInvite.inviter.tag}`);
        if (joinChannel) {
          await joinChannel.send({
            embeds: [
              new EmbedBuilder()
                .setColor(0xed4245)
                .setTitle("🔄 Player Rejoined — Invite Not Counted")
                .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
                .setDescription(
                  `**${member.user.username}** rejoined the server.\n` +
                  `⚠️ Invite was **not counted** for <@${inviterId}> — rejoin detected.`
                )
                .setFooter({ text: `Members: ${member.guild.memberCount}` })
                .setTimestamp(),
            ],
          });
        }
      } else {
        userInvites[inviterId] = (userInvites[inviterId] || 0) + 1;
        const total = userInvites[inviterId];
        console.log(`📥 ${member.user.tag} joined via ${usedInvite.inviter.tag} — ${total} invite(s) total`);

        if (joinChannel) {
          await joinChannel.send({
            embeds: [
              new EmbedBuilder()
                .setColor(0x57f287)
                .setTitle("👋 New Member Joined!")
                .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
                .setDescription(
                  `**${member.user.username}** was invited by <@${inviterId}>\n` +
                  `<@${inviterId}> now has **${total}** invite${total === 1 ? "" : "s"}.`
                )
                .setFooter({ text: `Members: ${member.guild.memberCount}` })
                .setTimestamp(),
            ],
          });
        }
      }
    } else {
      if (joinChannel) {
        await joinChannel.send({
          embeds: [
            new EmbedBuilder()
              .setColor(0xfee75c)
              .setTitle(isRejoin ? "🔄 Player Rejoined" : "👋 New Member Joined!")
              .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
              .setDescription(
                isRejoin
                  ? `**${member.user.username}** rejoined — inviter unknown.`
                  : `**${member.user.username}** joined — inviter couldn't be detected.`
              )
              .setFooter({ text: `Members: ${member.guild.memberCount}` })
              .setTimestamp(),
          ],
        });
      }
    }
  } catch (e) {
    console.error("guildMemberAdd error:", e.message);
  }
});

// ─── MEMBER LEAVE ─────────────────────────────────────────────────────────────
client.on("guildMemberRemove", async (member) => {
  joinedBefore.add(member.id);
  try {
    const newInvites = await member.guild.invites.fetch();
    const updatedCache = {};
    newInvites.forEach((inv) => {
      updatedCache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
    });
    inviteCache.set(member.guild.id, updatedCache);
  } catch (e) {
    console.error("guildMemberRemove error:", e.message);
  }
});

// ─── COMMANDS (messageCreate) ─────────────────────────────────────────────────
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
      const reply = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => reply.delete().catch(() => {}), 5000);
      return;
    }

    await message.delete().catch(() => {});

    const embed = new EmbedBuilder()
      .setColor(0x00c8ff)
      .setTitle("🎁 Invite Rewards")
      .setDescription(
        `Have you invited people to the server? Click **Claim Rewards** below!\n\n` +
        `**Rate:** 1 invite = ${REWARD_PER_INVITE}m\n\n` +
        `Your invite count is checked **automatically** — just enter your Minecraft IGN.`
      )
      .setFooter({ text: "Invite Rewards System" });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId("claim_reward")
        .setLabel("🎁 Claim Rewards")
        .setStyle(ButtonStyle.Primary)
    );

    await message.channel.send({ embeds: [embed], components: [row] });
    return;
  }

  // ── !mcreconnect — manually force a reconnect ────────────────────────────
  if (command === "!mcreconnect") {
    if (!isAdmin) {
      const reply = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => reply.delete().catch(() => {}), 5000);
      return;
    }

    await message.delete().catch(() => {});
    await spawnMinecraftBot(message.channel);
    return;
  }

  // ── !mcstatus ──────────────────────────────────────────────────────────────
  if (command === "!mcstatus") {
    if (!isAdmin) return;
    if (!mcBot) {
      await message.reply("🔴 Minecraft bot is **not connected**. Use `!spawn` to start it.");
    } else if (!mcReady) {
      await message.reply("🟡 Minecraft bot is **connecting...**");
    } else {
      await message.reply(`🟢 Minecraft bot is **online** as \`${mcBot.username}\` on \`${MC_HOST}:${MC_PORT}\``);
    }
    return;
  }

  // ── !mckick ────────────────────────────────────────────────────────────────
  if (command === "!mckick") {
    if (!isAdmin) return;
    if (mcBot) {
      mcBot.quit();
      mcBot   = null;
      mcReady = false;
      await message.reply("✅ Minecraft bot has been disconnected.");
    } else {
      await message.reply("❌ No Minecraft bot is currently connected.");
    }
    return;
  }
});

// ─── Spawn the Mineflayer bot ─────────────────────────────────────────────────
async function spawnMinecraftBot(feedbackChannel) {
  if (mcBot) {
    mcBot.quit();
    mcBot   = null;
    mcReady = false;
  }

  if (feedbackChannel) {
    await feedbackChannel.send("🔄 Connecting Minecraft bot... Check your Microsoft account for a verification code if prompted!");
  }

  try {
    mcBot = mineflayer.createBot({
      host:     MC_HOST,
      port:     MC_PORT,
      username: MC_USERNAME,
      auth:     "microsoft",
      version:  "1.20.5",
    });

    // Microsoft device code auth — send link to Discord or just log it
    mcBot.on("microsoftDeviceCode", async (data) => {
      const msg =
        `🔐 **Microsoft Verification Required**\n\n` +
        `1. Go to: **${data.verificationUri}**\n` +
        `2. Enter code: \`${data.userCode}\`\n` +
        `3. Sign in with the Microsoft account linked to your Minecraft Java account\n\n` +
        `The bot will join automatically once verified!`;

      console.log(`🔐 Microsoft verification needed — code: ${data.userCode} at ${data.verificationUri}`);

      if (feedbackChannel) {
        await feedbackChannel.send(msg);
      } else {
        // Find the reward log channel to post the verification message
        const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (logChannel) await logChannel.send(msg);
      }
    });

    mcBot.once("spawn", async () => {
      mcReady = true;
      console.log(`🎮 Minecraft bot spawned as ${mcBot.username} on ${MC_HOST}:${MC_PORT}`);

      const successMsg = `✅ Minecraft bot **${mcBot.username}** has joined **${MC_HOST}** and is ready to pay rewards!`;
      if (feedbackChannel) {
        await feedbackChannel.send(successMsg);
      } else {
        const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (logChannel) await logChannel.send(successMsg);
      }

      // Simulate basic human behaviour so anti-bot doesn't kick us
      // Look around slowly after spawning
      setTimeout(() => {
        if (mcBot && mcReady) {
          mcBot.look(mcBot.entity.yaw + 0.3, mcBot.entity.pitch, false);
        }
      }, 2000);
      setTimeout(() => {
        if (mcBot && mcReady) {
          mcBot.look(mcBot.entity.yaw - 0.1, 0, false);
        }
      }, 5000);

      // Process any queued payments that built up while offline
      if (payQueue.length > 0) {
        console.log(`⏳ Processing ${payQueue.length} queued payment(s)...`);
        runPayQueue();
      }
    });

    mcBot.on("kicked", async (reason) => {
      mcReady = false;
      // reason can be a string or a JSON chat object — convert it properly
      let reasonText = reason;
      try {
        const parsed = typeof reason === "string" ? JSON.parse(reason) : reason;
        reasonText = parsed?.text || parsed?.translate || JSON.stringify(parsed);
      } catch { reasonText = String(reason); }

      console.log(`⚠️ Minecraft bot was kicked: ${reasonText}`);
      const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
      if (logChannel) await logChannel.send(`⚠️ Minecraft bot was **kicked**: \`${reasonText}\` — attempting reconnect in 60s...`);

      // Wait 60 seconds before reconnecting to avoid spam kicks
      mcBot = null;
      setTimeout(() => spawnMinecraftBot(null), 60000);
    });

    mcBot.on("error", async (err) => {
      mcReady = false;
      console.error("Minecraft bot error:", err.message);
    });

    mcBot.on("end", async () => {
      if (mcReady) {
        // Only log if it was previously connected (not a manual quit)
        mcReady = false;
        console.log("Minecraft bot disconnected — attempting reconnect in 30s...");
        setTimeout(() => spawnMinecraftBot(null), 30000);
      }
    });

  } catch (e) {
    console.error("Failed to start Minecraft bot:", e.message);
    if (feedbackChannel) await feedbackChannel.send(`❌ Failed to start Minecraft bot: \`${e.message}\``);
  }
}

// ─── Run pay queue (one command every 1.5 seconds to avoid spam kick) ─────────
async function runPayQueue() {
  if (payRunning || payQueue.length === 0) return;
  payRunning = true;

  while (payQueue.length > 0) {
    if (!mcBot || !mcReady) {
      console.log("⚠️ MC bot not ready — pausing pay queue");
      break;
    }

    const { ign, amount } = payQueue.shift();
    const cmd = `/pay ${ign} ${amount}m`;

    try {
      mcBot.chat(cmd);
      console.log(`💰 Sent: ${cmd}`);
    } catch (e) {
      console.error(`Failed to send pay command for ${ign}:`, e.message);
    }

    // Wait 1.5s between commands so the server doesn't kick the bot for spam
    await new Promise((r) => setTimeout(r, 1500));
  }

  payRunning = false;
}

// ─── INTERACTIONS ─────────────────────────────────────────────────────────────
client.on("interactionCreate", async (interaction) => {

  // ── Claim button → modal ───────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "claim_reward") {
    const modal = new ModalBuilder()
      .setCustomId("reward_modal")
      .setTitle("Claim Your Invite Rewards");

    modal.addComponents(
      new ActionRowBuilder().addComponents(
        new TextInputBuilder()
          .setCustomId("ign")
          .setLabel("Your Minecraft Username")
          .setStyle(TextInputStyle.Short)
          .setPlaceholder("e.g. Steve")
          .setRequired(true)
      )
    );

    await interaction.showModal(modal);
    return;
  }

  // ── Modal submit ───────────────────────────────────────────────────────────
  if (interaction.isModalSubmit() && interaction.customId === "reward_modal") {
    await interaction.deferReply({ ephemeral: true });

    const ign        = interaction.fields.getTextInputValue("ign").trim();
    const userId     = interaction.user.id;
    const discordTag = interaction.user.tag;

    const total     = userInvites[userId]    || 0;
    const claimed   = claimedInvites[userId] || 0;
    const claimable = total - claimed;

    if (pendingRewards[userId]) {
      const p = pendingRewards[userId];
      await interaction.editReply({
        content:
          `⏳ You already have a pending claim!\n\n` +
          `**IGN:** ${p.ign} | **Invites:** ${p.invites} | **Reward:** ${p.invites * REWARD_PER_INVITE}m\n\n` +
          `Wait for it to be paid before claiming again.`,
      });
      return;
    }

    if (claimable <= 0) {
      await interaction.editReply({
        content:
          `❌ You have no unclaimed invites.\n\n` +
          `📊 Total invites: **${total}** | Already claimed: **${claimed}**\n\n` +
          `Invite more players to earn rewards!`,
      });
      return;
    }

    pendingRewards[userId] = { ign, invites: claimable, discordTag };
    await updateRewardList(interaction.client, interaction.guild);

    await interaction.editReply({
      content:
        `✅ **Claim submitted!**\n\n` +
        `🎮 **Minecraft IGN:** ${ign}\n` +
        `🎟️ **Invites:** ${claimable}\n` +
        `💰 **Reward:** ${claimable * REWARD_PER_INVITE}m\n\n` +
        `You'll get a DM when your reward has been sent in-game! 🎉`,
    });
    return;
  }

  // ── Mark individual paid ───────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId.startsWith("mark_paid_")) {
    if (
      !interaction.member.permissions.has(PermissionFlagsBits.Administrator) &&
      interaction.guild.ownerId !== interaction.user.id
    ) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }

    const targetId = interaction.customId.replace("mark_paid_", "");
    if (!pendingRewards[targetId]) {
      await interaction.reply({ content: "❌ Not found in pending list.", ephemeral: true });
      return;
    }

    const data   = pendingRewards[targetId];
    const amount = data.invites * REWARD_PER_INVITE;

    if (!mcBot || !mcReady) {
      await interaction.reply({
        content: `⚠️ Minecraft bot is not online! Payment queued for **${data.ign}** (${amount}m).\nUse \`!spawn\` to connect the bot and it will pay automatically.`,
        ephemeral: true,
      });
      payQueue.push({ ign: data.ign, amount });
    } else {
      payQueue.push({ ign: data.ign, amount });
      runPayQueue();
      await interaction.reply({ content: `✅ Paying **${data.ign}** ${amount}m in-game now!`, ephemeral: true });
    }

    await payUser(interaction.client, interaction.guild, targetId);
    await updateRewardList(interaction.client, interaction.guild);
    return;
  }

  // ── Pay all ────────────────────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "pay_all") {
    if (
      !interaction.member.permissions.has(PermissionFlagsBits.Administrator) &&
      interaction.guild.ownerId !== interaction.user.id
    ) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }

    const all = Object.keys(pendingRewards);
    if (all.length === 0) {
      await interaction.reply({ content: "✅ Nothing pending!", ephemeral: true });
      return;
    }

    // Queue all payments
    for (const uid of all) {
      const data   = pendingRewards[uid];
      const amount = data.invites * REWARD_PER_INVITE;
      payQueue.push({ ign: data.ign, amount });
    }

    const botStatus = (!mcBot || !mcReady)
      ? `\n\n⚠️ Minecraft bot is **offline** — payments have been queued. Use \`!spawn\` to connect it and they'll be sent automatically.`
      : "";

    // Pay everyone (DM + clear list)
    for (const uid of all) {
      await payUser(interaction.client, interaction.guild, uid);
    }

    await updateRewardList(interaction.client, interaction.guild);

    if (mcBot && mcReady) runPayQueue();

    await interaction.reply({
      content: `✅ Queued ${all.length} payment(s) — in-game commands are being sent now!${botStatus}`,
      ephemeral: true,
    });
    return;
  }
});

// ─── Pay one user (DM + clear pending) ────────────────────────────────────────
async function payUser(client, guild, userId) {
  const data = pendingRewards[userId];
  if (!data) return;

  claimedInvites[userId] = (claimedInvites[userId] || 0) + data.invites;

  try {
    const member = await guild.members.fetch(userId);
    await member.send(
      `✅ **Your invite rewards are being paid in-game!**\n\n` +
      `🎮 **IGN:** ${data.ign}\n` +
      `🎟️ **Invites:** ${data.invites}\n` +
      `💰 **Amount:** ${data.invites * REWARD_PER_INVITE}m\n\n` +
      `Please leave a vouch in **#Vouches-rewards**! 🎉`
    );
  } catch {
    console.log(`⚠️ Could not DM ${userId}`);
  }

  delete pendingRewards[userId];
}

// ─── Build / update the payout list ──────────────────────────────────────────
async function updateRewardList(client, guild) {
  const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
  if (!logChannel) return console.error("❌ Reward log channel not found.");

  const unpaid = Object.entries(pendingRewards);

  const mcStatus = (!mcBot || !mcReady)
    ? "🔴 MC Bot Offline — auto-reconnecting..."
    : `🟢 MC Bot Online as \`${mcBot.username}\``;

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
    const chunks = chunkArray(unpaid.slice(0, 20), 4);
    for (const chunk of chunks) {
      components.push(
        new ActionRowBuilder().addComponents(
          chunk.map(([uid, data]) =>
            new ButtonBuilder()
              .setCustomId(`mark_paid_${uid}`)
              .setLabel(`✅ ${data.ign}`)
              .setStyle(ButtonStyle.Success)
          )
        )
      );
    }

    components.push(
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId("pay_all")
          .setLabel("💰 Pay All & Clear Queue")
          .setStyle(ButtonStyle.Danger)
      )
    );
  }

  if (rewardListMessageId) {
    try {
      const existing = await logChannel.messages.fetch(rewardListMessageId);
      await existing.edit({ embeds: [embed], components });
      return;
    } catch {
      // message deleted, send new
    }
  }

  const sent = await logChannel.send({ embeds: [embed], components });
  rewardListMessageId = sent.id;
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

client.login(BOT_TOKEN);
