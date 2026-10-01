"""Gera o definitions.json do RabbitMQ juntando topologia.json com os usuários do .env.

Assim nenhuma senha fica escrita no repositório: o RabbitMQ recebe só o hash
(salt + SHA-256), que é o formato que ele importa.
"""
import base64
import hashlib
import json
import os
import secrets
import sys

TOPOLOGIA = "/rabbitmq/topologia.json"
SAIDA = "/definitions/definitions.json"

# papel -> prefixo das variáveis no .env (<PREFIXO>_USER e <PREFIXO>_PASS)
USUARIOS = {
    "admin": "RABBITMQ_ADMIN",
    "api": "API_PEDIDOS_RABBITMQ",
    "estoque": "ESTOQUE_RABBITMQ",
    "pagamento": "PAGAMENTO_RABBITMQ",
    "logistica": "LOGISTICA_RABBITMQ",
}


def env(nome: str) -> str:
    valor = os.environ.get(nome, "").strip()
    if not valor:
        sys.exit(f"variável obrigatória ausente no .env: {nome}")
    return valor


def hash_senha(senha: str) -> str:
    salt = secrets.token_bytes(4)
    return base64.b64encode(salt + hashlib.sha256(salt + senha.encode()).digest()).decode()


with open(TOPOLOGIA, encoding="utf-8") as f:
    topologia = json.load(f)
vhost = env("RABBITMQ_VHOST")

users, permissions = [], []
for papel, prefixo in USUARIOS.items():
    usuario = env(f"{prefixo}_USER")
    users.append({
        "name": usuario,
        "password_hash": hash_senha(env(f"{prefixo}_PASS")),
        "hashing_algorithm": "rabbit_password_hashing_sha256",
        "tags": ["administrator"] if papel == "admin" else [],
    })
    permissions.append({"user": usuario, "vhost": vhost, **topologia["permissoes"][papel]})

definitions = {
    "users": users,
    "vhosts": [{"name": vhost}],
    "permissions": permissions,
    "exchanges": [{**e, "vhost": vhost} for e in topologia["exchanges"]],
    "queues": [{**q, "vhost": vhost} for q in topologia["queues"]],
    "bindings": [{**b, "vhost": vhost} for b in topologia["bindings"]],
}

with open(SAIDA, "w", encoding="utf-8") as f:
    json.dump(definitions, f, indent=2)
print(f"definitions.json gerado: vhost '{vhost}', usuários {[u['name'] for u in users]}")
