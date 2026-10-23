# 🇬🇧 My GDrive Client for Linux 🚀☁️

*(Para a versão em português, role a página para baixo / Scroll down for the Portuguese version)*

A native, lightweight, and high-performance Google Drive synchronization client for Linux (focused on Mint, Ubuntu, and derivatives), built entirely in Go.

Unlike commercial clients that consume heavy RAM and CPU, this project was designed to be **invisible and efficient**. It mounts your Google Drive directly into the Linux file system via FUSE, using a local SQLite database for ultra-fast indexing and a minimalist interface in the System Tray.

## ✨ Key Features

* **Native Integration (FUSE):** Your Google Drive appears as a physical drive in Nemo, Nautilus, Dolphin, or any file manager.

* **Lightning Fast Boot (SQLite WAL):** The folder structure is cached locally. Full cloud scanning occurs only on the first access; subsequent boots are instant.

* **Incremental Sync (Polling):** Silently queries Google's *Changes API* every 30 seconds, downloading only what changed remotely.

* **System Tray Integrated:** Native taskbar icon for easy folder access and safe shutdown.

* **Smart Trash:** Blocks the creation of local `.Trash` folders. Deletions go straight to the actual cloud trash.

* **Smart Cache Management (15 GB):** Opened files are downloaded temporarily. A built-in Garbage Collector automatically deletes the oldest files when the cache limit is reached, protecting your local storage.

## 🛠️ Prerequisites and Dependencies

To run or compile this project, you need **Go (1.20+)** and the following system dependencies (required for FUSE and GTK GUI):

```
# Update your packages
sudo apt update

# Install FUSE 3 and graphical libraries (CGO/GTK)
sudo apt install fuse3 libfuse3-dev gcc libgtk-3-dev libayatana-appindicator3-dev

```

## 🔑 API Configuration (Bring Your Own Key)

For security and to avoid Rate Limits, this project requires you to use your own free Google Cloud API quota.

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).

2. Create a new project (e.g., `My Personal GDrive`).

3. Under **APIs & Services > Library**, enable the **Google Drive API**.

### ⚠️ Important: Bypassing the 7-Day Token Expiration

By default, Google puts new apps in "Testing" mode, meaning your login expires every 7 days. To fix this, you must "Publish" the app for your own use:

4. Go to **OAuth consent screen**.

5. The **Publish App** button under the "Audience" menu will likely be disabled. To enable it, go to the **Branding** menu on the left side and fill in the minimum required fields:

   * **App Name:** My GDrive Client

   * **Support/Developer Email:** Use your *own personal email*.

   * **Homepage & Privacy Policy Links:** You can safely paste the URL of this GitHub repository.

   * **Authorized domains:** Add `github.com` (this is required by Google to validate the repository links).

6. Save the Branding page, return to the **Audience** menu, and click **Publish App** (status will change to Production).

7. ⚠️ **Important — Ignore Verification Prompts:** Google will display a yellow banner stating *"Your app needs to be verified. Submit for review"*. **DO NOT submit the app for verification.** Since you are running your own open-source code with your own key, publishing without verification is completely safe and fully removes the 7-day expiration limit.

8. Finally, go to **Credentials > Create Credentials > OAuth client ID**. Choose **Desktop App**.

9. Download the generated JSON file, rename it to `credentials.json`, and place it in the app's hidden config folder: `~/.config/meu-gdrive-client/credentials.json`.

## ⚙️ Local Cache Configuration

**Default Size:** The cache limit is **15 GB**.
**How to change:** You can set a custom size by exporting the `GDRIVE_CACHE_GB` environment variable. Example for 30 GB:

```
export GDRIVE_CACHE_GB=30

```

## 🚀 How to Build and Use

1. **Clone the repository:**

   ```
   git clone https://github.com/allanpaolin/meu-gdrive-client.git
   cd meu-gdrive-client
   go mod tidy
   
   ```

2. **Create the mount folder:**

   ```
   mkdir ~/GoogleDrive
   
   ```

3. **Build the optimized executable:**

   ```
   go build -ldflags "-s -w" -o meu-gdrive-client
   
   ```

4. **Start the app:**

   ```
   ./meu-gdrive-client
   
   ```

   *On the first run, it will open your web browser to log in. Because you haven't verified the app with Google, a red warning screen will appear. This is normal. Click **Advanced** -> **Go to app (unsafe)** to proceed.*

### 🖥️ Autostart on Linux

Add the generated executable to your Linux Mint/Ubuntu **Startup Applications**. The program will run invisibly in the background every time you boot.

## 🔒 Privacy Policy

**My GDrive Client** is an open-source tool executed entirely on your local machine. It does not collect, track, or transmit any personal information, files, or metadata to third-party servers. Communication happens directly between your PC and Google Drive's official servers. Your credentials are saved securely in your local OS Keyring.

# 🇧🇷 Meu GDrive Client para Linux 🚀☁️

Um cliente nativo, leve e de alta performance de sincronização do Google Drive para Linux (focado em Mint, Ubuntu e derivados), construído totalmente em Go.

Diferente de clientes comerciais que consomem muita RAM e CPU, este projeto foi desenhado para ser **invisível e eficiente**. Ele monta o seu Google Drive diretamente no sistema de arquivos do Linux via FUSE, utilizando um banco de dados SQLite local para indexação ultrarrápida e uma interface minimalista na bandeja do sistema (System Tray).

## ✨ Principais Funcionalidades

* **Integração Nativa (FUSE):** O seu Google Drive aparece como um disco físico (pendrive) no Nemo, Nautilus, Dolphin ou qualquer outro gerenciador de arquivos. Permite criar, editar, mover e apagar arquivos nativamente.

* **Boot a Jato (SQLite com WAL):** A estrutura de pastas é cacheada localmente. A varredura completa da nuvem ocorre apenas no primeiro acesso; nas próximas inicializações, o carregamento é instantâneo.

* **Sincronização Incremental (Polling):** Um processo silencioso consulta a *Changes API* do Google a cada 30 segundos, baixando apenas o que foi alterado remotamente.

* **System Tray Integrado:** Ícone nativo na barra de tarefas para controle fácil e encerramento seguro.

* **Lixeira Inteligente:** Bloqueia a criação de pastas `.Trash` locais do Linux. Exclusões são enviadas diretamente para a lixeira real da nuvem.

* **Gestão Inteligente de Cache (15 GB):** Um *Garbage Collector* embutido apaga automaticamente os arquivos mais antigos quando o cache local atinge o limite definido, protegendo seu HD.

## 🛠️ Pré-requisitos e Dependências

Para rodar ou compilar este projeto, você precisará do **Go (1.20+)** e das seguintes dependências de sistema (necessárias para o FUSE e GTK):

```
# Atualize seus pacotes
sudo apt update

# Instale o FUSE 3 e as bibliotecas gráficas (CGO/GTK)
sudo apt install fuse3 libfuse3-dev gcc libgtk-3-dev libayatana-appindicator3-dev

```

## 🔑 Configuração da API (Bring Your Own Key)

Por segurança e para evitar limites de taxa (Rate Limits), este projeto exige que você utilize a sua própria cota gratuita da API do Google Cloud.

1. Acesse o [Google Cloud Console](https://console.cloud.google.com/).

2. Crie um projeto (ex: `Meu GDrive Pessoal`).

3. Em **APIs e Serviços > Biblioteca**, ative a **Google Drive API**.

### ⚠️ Atenção: Bypasse a Expiração de 7 Dias do Token

Por padrão, o Google coloca novos aplicativos no modo "Teste", o que faz com que a sua autorização expire a cada 7 dias. Para evitar isso e tornar o login definitivo, você precisa publicar o app:

4. Acesse a **Tela de Permissão OAuth** no Google Cloud.

5. Vá no menu **Público-alvo**. O botão **Publicar Aplicativo** provavelmente estará apagado (desabilitado). Para ativá-lo, vá primeiro no menu lateral **Branding** e preencha os dados obrigatórios:

   * **Nome do app:** Meu GDrive Client

   * **E-mails de suporte/desenvolvedor:** Preencha com o seu *próprio e-mail pessoal*.

   * **Página inicial e Política de Privacidade:** Insira o link do repositório do GitHub deste projeto.

   * **Domínios autorizados:** Adicione `github.com` (isso é obrigatório para o Google aceitar os links do repositório).

6. Salve a tela de Branding. Volte no menu **Público-alvo** e agora clique em **Publicar Aplicativo** (o status mudará para Produção).

7. ⚠️ **Atenção — Ignore o aviso de Verificação:** O Google exibirá uma faixa amarela dizendo *"Seu aplicativo precisa ser verificado. Envie o aplicativo para análise"*. **NÃO envie o aplicativo para verificação.** Esse aviso é apenas um alerta para apps comerciais públicos. Como você está rodando seu próprio código com sua própria chave, publicar sem verificar é seguro e suficiente para remover o limite de 7 dias.

8. Vá em **Credenciais > Criar Credenciais > ID do cliente OAuth**. Escolha **App para Computador** (*Desktop App*).

9. Faça o download do arquivo JSON gerado. Renomeie-o para `credentials.json` e coloque-o na **pasta oculta de configuração do aplicativo no seu sistema**: `~/.config/meu-gdrive-client/credentials.json`.

## ⚙️ Configuração de Cache Local

O aplicativo gerencia os downloads temporários no seu disco.

**Tamanho Padrão:** O limite de cache é de **15 GB**.
**Como alterar:** Você pode definir um tamanho personalizado exportando a variável de ambiente `GDRIVE_CACHE_GB`. Exemplo para 30 GB:

```
export GDRIVE_CACHE_GB=30

```

## 🚀 Como Compilar e Usar

1. **Clone o repositório:**

   ```
   git clone https://github.com/allanpaolin/meu-gdrive-client.git
   cd meu-gdrive-client
   go mod tidy
   
   ```

2. **Crie a pasta de montagem:**

   ```
   mkdir ~/GoogleDrive
   
   ```

3. **Gere o executável otimizado:**

   ```
   go build -ldflags "-s -w" -o meu-gdrive-client
   
   ```

4. **Inicie o aplicativo:**

   ```
   ./meu-gdrive-client
   
   ```

   *No primeiro acesso, o navegador abrirá pedindo login. Como você não pagou pela auditoria oficial do Google, uma tela vermelha de alerta ("O Google não verificou este app") aparecerá. Isso é normal, pois o código está rodando localmente com a sua credencial. Clique em **Avançado** -> **Acessar (não seguro)**.*

### 🖥️ Iniciando com o Linux (Autostart)

Adicione o executável gerado aos **Aplicativos de Inicialização** (Startup Applications) do seu Linux Mint/Ubuntu. O programa rodará em background assim que o computador ligar.

## 🔒 Política de Privacidade

O **Meu GDrive Client** é uma ferramenta de código aberto executada integralmente na sua máquina local. O aplicativo não coleta, rastreia ou transmite qualquer informação pessoal, arquivo ou metadado para servidores de terceiros. A comunicação ocorre diretamente entre o seu computador e os servidores do Google. As suas chaves de acesso são armazenadas no cofre seguro do seu sistema operacional (Keyring). Qualquer usuário pode auditar o código-fonte deste repositório.