from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Sequence

from dotenv import load_dotenv

from src.config import Config, load_config
from src.runtime import RateLimiter, StepRunner, logger, register_signal_handlers, setup_logging
from src.steps import build_steps


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lima content pipeline")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.yaml"),
        help="Path to the config.yaml file",
    )
    parser.add_argument(
        "--step",
        type=str,
        help="Run only a specific step (by name or index). Use comma to select multiple.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Override thread pool size",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare jobs without execution",
    )
    return parser.parse_args(argv)


def select_steps(all_steps: List, selector: str | None) -> List:
    if not selector:
        return all_steps

    indices = {str(idx + 1): step for idx, step in enumerate(all_steps)}
    names = {step.NAME: step for step in all_steps}

    selected: List = []
    for token in selector.split(","):
        token = token.strip()
        if not token:
            continue
        step = names.get(token) or indices.get(token)
        if step:
            selected.append(step)
        else:
            logger.warning("Unknown step selector", selector=token)
    return selected


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()

    config = load_config(args.config)
    if args.threads:
        config = config.model_copy(update={"threads": args.threads})

    setup_logging(config.logging)
    steps = build_steps(config)
    selected_steps = select_steps(steps, args.step)

    if not selected_steps:
        logger.info("No steps selected for execution")
        return 0

    for step in selected_steps:
        if not step.should_run():
            logger.info("Step disabled in config", step=step.NAME)
            continue

        logger.info("Loading jobs", step=step.NAME)
        jobs = step.load_jobs()
        total_jobs = len(jobs)
        if total_jobs == 0:
            logger.info("No jobs to process", step=step.NAME)
            continue

        if args.dry_run:
            logger.info(
                "Dry-run mode: skipping execution",
                step=step.NAME,
                jobs=total_jobs,
            )
            continue

        step_config = config
        if getattr(step, "NAME", "") == "generate_audio":
            step_config = config.model_copy(update={"threads": config.threads_audio})
        elif getattr(step, "NAME", "") == "compose_music":
            step_config = config.model_copy(update={"threads": config.threads_music})

        rate_limiter = RateLimiter(step_config.rate_limits)
        runner = StepRunner(step, step_config, rate_limiter=rate_limiter)
        register_signal_handlers(runner)

        logger.info("[BUSINESS] Step started | jobs={}", total_jobs, step=step.NAME)
        progress = runner.run(jobs)
        snapshot = progress.snapshot()
        logger.info(
            "[BUSINESS] Step completed | total={} done={} failed={} skipped={} elapsed={:.2f}s",
            snapshot.total,
            snapshot.completed,
            snapshot.failed,
            snapshot.skipped,
            snapshot.elapsed_sec or 0.0,
            step=step.NAME,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
